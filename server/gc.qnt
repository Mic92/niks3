// -*- mode: Bluespec; -*-
// Model of the push/GC protocol in server/pending_closure.go,
// server/objects_model.go and server/pg/query.sql.
//
//   quint verify server/gc.qnt --invariant safe --inductive-invariant indInv
//
// SQL statements are atomic steps. The reaper transaction is split at the
// points where it waits for S3, because that is where its row locks matter.
module gc {
  type Key = str
  type Push = str

  pure val KEYS: Set[Key] = Set("root1", "root2", "nar", "log")
  pure val PUSHES: Set[Push] = Set("p1", "p2")

  // Static reference graph: narinfo -> nar, log. Two roots share objects.
  pure def refs(k: Key): Set[Key] =
    if (k == "root1") Set("nar", "log")
    else if (k == "root2") Set("nar")
    else Set()

  pure def reach(roots: Set[Key]): Set[Key] =
    // depth 1 graph, so one step is the fixpoint
    roots.union(roots.map(r => refs(r)).flatten())

  // ---------------------------------------------------------------- state

  var s3: Set[Key]
  // objects rows, split by deleted_at IS NULL / IS NOT NULL
  var live: Set[Key]
  var tomb: Set[Key]
  var closures: Set[Key]

  // pending_closures + client progress.
  //   idle:       no pending closure
  //   registered: pending_objects committed (registerPendingClosure)
  //   uploading:  GetLiveObjects ran, client uploads `todo`
  type PushState = { phase: str, root: Key, todo: Set[Key] }
  var push: Push -> PushState

  // Reaper transaction (reapBatch). Only one GC runs (advisory lock).
  //   idle:      no open reap tx; mark may run
  //   locked:    LockObjectsForDeletion returned, rows FOR UPDATE
  //   rechecked: GetPendingKeysAmong ran, deleting `toDelete` from S3
  type GcState = { phase: str, locked: Set[Key], toDelete: Set[Key], deleted: Set[Key] }
  var gc: GcState

  // -------------------------------------------------------------- helpers

  def keysOf(p: Push): Set[Key] =
    if (push.get(p).phase == "idle") Set() else reach(Set(push.get(p).root))

  def pendingKeys: Set[Key] = PUSHES.map(p => keysOf(p)).flatten()

  val noGc: GcState = { phase: "idle", locked: Set(), toDelete: Set(), deleted: Set() }

  // -------------------------------------------------------------- actions

  action init = all {
    s3' = Set(),
    live' = Set(),
    tomb' = Set(),
    closures' = Set(),
    push' = PUSHES.mapBy(_ => { phase: "idle", root: "root1", todo: Set() }),
    gc' = noGc,
  }

  // registerPendingClosure: pending rows for every closure object.
  action register(p: Push, root: Key): bool = all {
    push.get(p).phase == "idle",
    push' = push.set(p, { phase: "registered", root: root, todo: Set() }),
    s3' = s3, live' = live, tomb' = tomb, closures' = closures, gc' = gc,
  }

  // keysToUpload / GetLiveObjects ... FOR SHARE: blocks on reaper locks,
  // tombstoned counts as absent.
  action checkLive(p: Push): bool = all {
    push.get(p).phase == "registered",
    keysOf(p).intersect(gc.locked) == Set(),
    push' = push.set(p, { ...push.get(p), phase: "uploading", todo: keysOf(p).exclude(live) }),
    s3' = s3, live' = live, tomb' = tomb, closures' = closures, gc' = gc,
  }

  action upload(p: Push, k: Key): bool = all {
    push.get(p).phase == "uploading",
    push.get(p).todo.contains(k),
    s3' = s3.union(Set(k)),
    push' = push.set(p, { ...push.get(p), todo: push.get(p).todo.exclude(Set(k)) }),
    live' = live, tomb' = tomb, closures' = closures, gc' = gc,
  }

  // commit_pending_closure: upsert every object as live. The upsert blocks
  // on reaper row locks.
  action commit(p: Push): bool = all {
    push.get(p).phase == "uploading",
    push.get(p).todo == Set(),
    keysOf(p).intersect(gc.locked) == Set(),
    closures' = closures.union(Set(push.get(p).root)),
    live' = live.union(keysOf(p)),
    tomb' = tomb.exclude(keysOf(p)),
    push' = push.set(p, { ...push.get(p), phase: "idle", todo: Set() }),
    s3' = s3, gc' = gc,
  }

  // CleanupPendingClosures: abandoned upload, whatever it may have put in S3
  // becomes a tombstone (ON CONFLICT DO NOTHING).
  action abandon(p: Push): bool = all {
    push.get(p).phase != "idle",
    tomb' = tomb.union(keysOf(p).exclude(live)),
    push' = push.set(p, { ...push.get(p), phase: "idle", todo: Set() }),
    s3' = s3, live' = live, closures' = closures, gc' = gc,
  }

  // Closure expiry (cleanupClosuresOlderThan).
  action dropClosure(k: Key): bool = all {
    closures.contains(k),
    closures' = closures.exclude(Set(k)),
    s3' = s3, live' = live, tomb' = tomb, push' = push, gc' = gc,
  }

  // MarkStaleObjects under REPEATABLE READ + retry: atomic w.r.t. commit.
  action mark: bool = {
    val stale = live.exclude(reach(closures)).exclude(pendingKeys)
    all {
      gc.phase == "idle",
      live' = live.exclude(stale),
      tomb' = tomb.union(stale),
      s3' = s3, closures' = closures, push' = push, gc' = gc,
    }
  }

  // LockObjectsForDeletion. The NOT EXISTS(pending) filter runs on the
  // statement snapshot and may miss concurrent inserts, so it is not
  // modelled at all: any tombstoned rows may get locked. deleted_at IS NOT
  // NULL is re-evaluated on the locked row version.
  action lockBatch(cand: Set[Key]): bool = all {
    gc.phase == "idle",
    cand != Set(),
    cand.subseteq(tomb),
    gc' = { phase: "locked", locked: cand, toDelete: Set(), deleted: Set() },
    s3' = s3, live' = live, tomb' = tomb, closures' = closures, push' = push,
  }

  // GetPendingKeysAmong with a fresh snapshot.
  action recheck: bool = all {
    gc.phase == "locked",
    gc' = { ...gc, phase: "rechecked", toDelete: gc.locked.exclude(pendingKeys) },
    s3' = s3, live' = live, tomb' = tomb, closures' = closures, push' = push,
  }

  // deleteFromS3, one key at a time; failures simply never take this step.
  action s3Delete(k: Key): bool = all {
    gc.phase == "rechecked",
    gc.toDelete.exclude(gc.deleted).contains(k),
    s3' = s3.exclude(Set(k)),
    gc' = { ...gc, deleted: gc.deleted.union(Set(k)) },
    live' = live, tomb' = tomb, closures' = closures, push' = push,
  }

  // DeleteTombstonedObjects for what is gone from S3, then COMMIT.
  action reapFinish: bool = all {
    gc.phase == "rechecked",
    tomb' = tomb.exclude(gc.deleted),
    gc' = noGc,
    s3' = s3, live' = live, closures' = closures, push' = push,
  }

  action step = any {
    nondet p = PUSHES.oneOf()
    nondet k = KEYS.oneOf()
    any { register(p, k), checkLive(p), upload(p, k), commit(p), abandon(p) },
    nondet k = KEYS.oneOf()
    any { dropClosure(k), s3Delete(k) },
    nondet cand = KEYS.powerset().oneOf()
    lockBatch(cand),
    mark, recheck, reapFinish,
  }

  // ----------------------------------------------------------- properties

  // Everything a committed closure references is in S3.
  val safe = reach(closures).subseteq(s3)

  val typeOK = all {
    KEYS.powerset().contains(s3),
    KEYS.powerset().contains(live),
    KEYS.powerset().contains(tomb),
    KEYS.powerset().contains(closures),
    PUSHES.setOfMaps(tuples(Set("idle", "registered", "uploading"), KEYS, KEYS.powerset())
      .map(((ph, r, t)) => { phase: ph, root: r, todo: t })).contains(push),
    tuples(Set("idle", "locked", "rechecked"), KEYS.powerset(), KEYS.powerset(), KEYS.powerset())
      .map(((ph, l, d, x)) => { phase: ph, locked: l, toDelete: d, deleted: x }).contains(gc),
  }

  val indInv = all {
    typeOK,
    live.intersect(tomb) == Set(),
    // live rows are backed by S3
    live.subseteq(s3),
    // mark never tombstones anything reachable, commit makes everything live
    reach(closures).subseteq(live),
    // the reaper only ever holds and deletes tombstoned rows
    gc.locked.subseteq(tomb),
    gc.toDelete.subseteq(gc.locked),
    gc.deleted.subseteq(gc.toDelete),
    gc.deleted.intersect(s3) == Set(),
    (gc.phase == "idle") == (gc.locked == Set()),
    gc.phase != "rechecked" implies gc.toDelete == Set(),
    PUSHES.forall(p => {
      val st = push.get(p)
      val done = keysOf(p).exclude(st.todo)
      all {
        st.phase != "uploading" implies st.todo == Set(),
        st.todo.subseteq(keysOf(p)),
        // what a push considers present is in S3, and a reaper past its
        // recheck has seen the pending rows
        st.phase == "uploading" implies done.subseteq(s3),
        st.phase == "uploading" implies keysOf(p).intersect(gc.toDelete) == Set(),
      }
    }),
  }
}
