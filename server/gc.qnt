// -*- mode: Bluespec; -*-
// Push vs. GC protocol of server/pending_closure.go, server/uploads.go,
// server/objects_model.go, server/pg/query.sql and
// server/pg/functions/1_commit_pending_closure.sql at PostgreSQL row level.
//
//   quint verify server/gc.qnt --invariant safe --inductive-invariant indInv
//
// proves `safe` for all interleavings of any length. For an explicit-state
// cross-check compile with `quint compile --target tlaplus` and run TLC.
//
// SQL semantics encoded:
//  * A transaction's writes become visible atomically at COMMIT, so a
//    transaction that never waits mid-way is one step.
//  * Statements that lock rows (SELECT .. FOR SHARE/UPDATE, UPDATE) take
//    them one at a time in index order and may wait between rows while
//    holding the ones already taken. Those are one step per row.
//  * READ COMMITTED lockers re-evaluate the row predicate on the latest
//    committed version (EvalPlanQual). Sub-selects keep the statement
//    snapshot, so LockObjectsForDeletion's NOT EXISTS(pending) is not
//    relied on and not modelled.
//  * A statement snapshot does not contain rows inserted after it was
//    taken. Such rows are neither seen nor locked.
//  * REPEATABLE READ UPDATE (mark) computes its targets from the transaction
//    snapshot and fails with 40001 on a target row written by a transaction
//    that committed after that snapshot. Merely locked rows are fine.
//  * INSERT .. ON CONFLICT DO UPDATE locks and writes the existing row.
//    DO NOTHING takes no lock.
//  * Deadlocks and cancellations abort the statement (readAbort, markAbort).
module gc {
  type Key = str
  type Push = str

  pure val KEYS: Set[Key] = Set("root1", "root2", "nar")
  pure val PUSHES: Set[Push] = Set("p1", "p2")

  pure def refs(k: Key): Set[Key] =
    if (k == "root1" or k == "root2") Set("nar") else Set()

  // Lockers visit rows in index order.
  pure def ord(k: Key): int = if (k == "nar") 0 else if (k == "root1") 1 else 2
  pure def lowest(ks: Set[Key]): Set[Key] = ks.filter(k => ks.forall(o => ord(o) >= ord(k)))

  pure def reach(roots: Set[Key]): Set[Key] = roots.union(roots.map(r => refs(r)).flatten())

  // ---------------------------------------------------------------- state

  var s3: Set[Key]
  var live: Set[Key]      // objects rows with deleted_at IS NULL
  var tomb: Set[Key]      // objects rows with deleted_at IS NOT NULL
  var closures: Set[Key]

  // One client push and its server-side pending_closure.
  //   cli:     idle | registered | reading (GetLiveObjects running) |
  //            uploading | failed | gone
  //   pending: pending_closures/pending_objects rows exist
  //   snap:    objects rows in GetLiveObjects' statement snapshot
  //   read:    keys GetLiveObjects has visited; slock: rows it holds FOR SHARE
  //   todo:    keys still to PUT to S3
  //   put:     keys PUT but not yet RegisterCompletedObject'ed
  type PushState = {
    cli: str, pending: bool, root: Key,
    snap: Set[Key], read: Set[Key], slock: Set[Key], todo: Set[Key], put: Set[Key],
  }
  // Stored column-wise so Apalache can enumerate the type invariant.
  var cli: Push -> str
  var pending: Push -> bool
  var root: Push -> Key
  var snap: Push -> Set[Key]
  var read: Push -> Set[Key]
  var slock: Push -> Set[Key]
  var todo: Push -> Set[Key]
  var putK: Push -> Set[Key]

  // markStaleObjects: REPEATABLE READ transaction.
  //   stale: target rows computed from the snapshot
  //   xlock: rows already updated (locked) in this attempt
  //   dirty: rows written by others since the snapshot
  type MarkState = { on: bool, stale: Set[Key], xlock: Set[Key], dirty: Set[Key] }
  var mark: MarkState

  // reapBatch transaction.
  //   phase: idle | locking (LockObjectsForDeletion taking rows) |
  //          deleting (after GetPendingKeysAmong, S3 deletes in flight)
  type ReapState = { phase: str, xlock: Set[Key], toDelete: Set[Key], deleted: Set[Key] }
  var reap: ReapState

  // -------------------------------------------------------------- helpers

  def st(p: Push): PushState = { cli: cli.get(p), pending: pending.get(p), root: root.get(p),
    snap: snap.get(p), read: read.get(p), slock: slock.get(p), todo: todo.get(p), put: putK.get(p) }
  action setPush(p: Push, r: PushState): bool = all {
    cli' = cli.set(p, r.cli), pending' = pending.set(p, r.pending), root' = root.set(p, r.root),
    snap' = snap.set(p, r.snap), read' = read.set(p, r.read), slock' = slock.set(p, r.slock),
    todo' = todo.set(p, r.todo), putK' = putK.set(p, r.put),
  }
  action keepPush = all {
    cli' = cli, pending' = pending, root' = root, snap' = snap, read' = read, slock' = slock, todo' = todo, putK' = putK,
  }
  def keysOf(p: Push): Set[Key] = if (st(p).cli == "idle") Set() else reach(Set(st(p).root))
  def pendingKeys: Set[Key] = PUSHES.filter(p => st(p).pending).map(p => reach(Set(st(p).root))).flatten()
  def rows: Set[Key] = live.union(tomb)
  def slockedBy(others: Set[Push]): Set[Key] = others.map(p => st(p).slock).flatten()
  def xlocked: Set[Key] = mark.xlock.union(reap.xlock)
  // Keys a writer needing an exclusive row lock on must find free.
  def busyFor(p: Push): Set[Key] = xlocked.union(slockedBy(PUSHES.exclude(Set(p))))
  def touch(ks: Set[Key]): MarkState = if (mark.on) { ...mark, dirty: mark.dirty.union(ks) } else mark

  val idlePush: PushState = { cli: "idle", pending: false, root: "root1",
    snap: Set(), read: Set(), slock: Set(), todo: Set(), put: Set() }
  val noMark: MarkState = { on: false, stale: Set(), xlock: Set(), dirty: Set() }
  val noReap: ReapState = { phase: "idle", xlock: Set(), toDelete: Set(), deleted: Set() }

  action keep = all { s3' = s3, live' = live, tomb' = tomb, closures' = closures }

  // ---------------------------------------------------------------- push

  action init = all {
    s3' = Set(), live' = Set(), tomb' = Set(), closures' = Set(),
    cli' = PUSHES.mapBy(_ => "idle"), pending' = PUSHES.mapBy(_ => false), root' = PUSHES.mapBy(_ => "root1"),
    snap' = PUSHES.mapBy(_ => Set()), read' = PUSHES.mapBy(_ => Set()), slock' = PUSHES.mapBy(_ => Set()),
    todo' = PUSHES.mapBy(_ => Set()), putK' = PUSHES.mapBy(_ => Set()),
    mark' = noMark, reap' = noReap,
  }

  // registerPendingClosure; COMMIT.
  action register(p: Push, root: Key): bool = all {
    st(p).cli == "idle",
    setPush(p, { ...idlePush, cli: "registered", pending: true, root: root }),
    keep, mark' = mark, reap' = reap,
  }

  // GetLiveObjects takes its statement snapshot.
  action readBegin(p: Push): bool = all {
    st(p).cli == "registered",
    setPush(p, { ...st(p), cli: "reading", snap: rows }),
    keep, mark' = mark, reap' = reap,
  }

  // GetLiveObjects visiting one key. Rows not in the snapshot are neither
  // locked nor reported; rows deleted since are skipped; otherwise the
  // latest committed version is locked and read.
  action readRow(p: Push, k: Key): bool = {
    val has = st(p).snap.contains(k) and rows.contains(k)
    all {
      st(p).cli == "reading",
      lowest(keysOf(p).exclude(st(p).read)).contains(k),
      not(has) or not(xlocked.contains(k)),
      setPush(p, { ...st(p),
        read: st(p).read.union(Set(k)),
        slock: if (has) st(p).slock.union(Set(k)) else st(p).slock,
        todo: if (has and live.contains(k)) st(p).todo else st(p).todo.union(Set(k)) }),
      keep, mark' = mark, reap' = reap,
    }
  }

  // GetLiveObjects aborted (deadlock victim, timeout): HTTP 500.
  action readAbort(p: Push): bool = all {
    st(p).cli == "reading",
    setPush(p, { ...idlePush, cli: "failed", pending: st(p).pending, root: st(p).root }),
    keep, mark' = mark, reap' = reap,
  }

  // GetLiveObjects returns; share locks released.
  action readDone(p: Push): bool = all {
    st(p).cli == "reading",
    st(p).read == keysOf(p),
    setPush(p, { ...st(p), cli: "uploading", slock: Set() }),
    keep, mark' = mark, reap' = reap,
  }

  // Presigned PUT.
  action s3Put(p: Push, k: Key): bool = all {
    st(p).cli == "uploading",
    st(p).todo.contains(k),
    s3' = s3.union(Set(k)),
    setPush(p, { ...st(p), todo: st(p).todo.exclude(Set(k)), put: st(p).put.union(Set(k)) }),
    live' = live, tomb' = tomb, closures' = closures, mark' = mark, reap' = reap,
  }

  // RegisterCompletedObject: one statement that locks this closure's
  // pending_objects row and upserts the objects row from it. With the
  // pending row gone (abandoned) nothing is written and the client fails.
  action register1(p: Push, k: Key): bool = all {
    st(p).cli == "uploading",
    st(p).put.contains(k),
    if (st(p).pending) all {
      not(busyFor(p).contains(k)),
      live' = live.union(Set(k)),
      tomb' = tomb.exclude(Set(k)),
      mark' = touch(Set(k)),
      setPush(p, { ...st(p), put: st(p).put.exclude(Set(k)) }),
    } else all {
      live' = live, tomb' = tomb, mark' = mark,
      setPush(p, { ...idlePush, cli: "failed", root: st(p).root }),
    },
    s3' = s3, closures' = closures, reap' = reap,
  }

  // commit_pending_closure; COMMIT. Raises if the pending closure is gone.
  action commit(p: Push): bool = all {
    st(p).cli == "uploading",
    st(p).todo == Set(), st(p).put == Set(),
    if (st(p).pending) all {
      keysOf(p).intersect(busyFor(p)) == Set(),
      closures' = closures.union(Set(st(p).root)),
      live' = live.union(keysOf(p)),
      tomb' = tomb.exclude(keysOf(p)),
      // the upsert writes every row, even unchanged ones, so that a
      // concurrent REPEATABLE READ mark conflicts
      mark' = touch(keysOf(p)),
    } else all {
      closures' = closures, live' = live, tomb' = tomb, mark' = mark,
    },
    setPush(p, idlePush),
    s3' = s3, reap' = reap,
  }

  // A failed client goes away.
  action clientExit(p: Push): bool = all {
    st(p).cli == "failed",
    setPush(p, { ...idlePush, cli: if (st(p).pending) "gone" else "idle",
      pending: st(p).pending, root: st(p).root }),
    keep, mark' = mark, reap' = reap,
  }

  // CleanupPendingClosures picks up this pending closure.
  action abandon(p: Push): bool = {
    val ks = reach(Set(st(p).root))
    val ins = ks.exclude(rows)
    all {
      st(p).pending,
      tomb' = tomb.union(ins),
      mark' = touch(ins),
      setPush(p, if (st(p).cli == "gone") idlePush else st(p).with("pending", false)),
      s3' = s3, live' = live, closures' = closures, reap' = reap,
    }
  }

  // ------------------------------------------------------------------ gc

  // DeleteClosures.
  action dropClosure(k: Key): bool = all {
    closures.contains(k),
    closures' = closures.exclude(Set(k)),
    s3' = s3, live' = live, tomb' = tomb, keepPush, mark' = mark, reap' = reap,
  }

  // BEGIN ISOLATION LEVEL REPEATABLE READ; snapshot taken.
  action markBegin: bool = all {
    not(mark.on), reap.phase == "idle",
    mark' = { on: true, stale: live.exclude(reach(closures)).exclude(pendingKeys), xlock: Set(), dirty: Set() },
    keep, keepPush, reap' = reap,
  }

  // UPDATE reaches one target row.
  action markRow(k: Key): bool = all {
    mark.on,
    lowest(mark.stale.exclude(mark.xlock)).contains(k),
    not(reap.xlock.contains(k)), not(slockedBy(PUSHES).contains(k)),
    if (mark.dirty.contains(k))
      mark' = noMark   // 40001, ROLLBACK, retry later
    else
      mark' = { ...mark, xlock: mark.xlock.union(Set(k)) },
    keep, keepPush, reap' = reap,
  }

  // ROLLBACK (deadlock victim, cancelled).
  action markAbort: bool = all {
    mark.on,
    mark' = noMark,
    keep, keepPush, reap' = reap,
  }

  // COMMIT.
  action markCommit: bool = all {
    mark.on,
    mark.xlock == mark.stale,
    live' = live.exclude(mark.stale),
    tomb' = tomb.union(mark.stale),
    mark' = noMark,
    s3' = s3, closures' = closures, keepPush, reap' = reap,
  }

  // LockObjectsForDeletion reaches one row; deleted_at IS NOT NULL is
  // re-evaluated on the current version.
  action reapLock(k: Key): bool = all {
    not(mark.on),
    reap.phase == "idle" or reap.phase == "locking",
    tomb.contains(k),
    reap.xlock.forall(o => ord(k) > ord(o)),
    not(slockedBy(PUSHES).contains(k)),
    reap' = { ...reap, phase: "locking", xlock: reap.xlock.union(Set(k)) },
    keep, keepPush, mark' = mark,
  }

  // GetPendingKeysAmong with a fresh snapshot.
  action reapRecheck: bool = all {
    reap.phase == "locking",
    reap' = { ...reap, phase: "deleting", toDelete: reap.xlock.exclude(pendingKeys) },
    keep, keepPush, mark' = mark,
  }

  // deleteFromS3 removes one key. Keys that fail just never take this step.
  action reapS3(k: Key): bool = all {
    reap.phase == "deleting",
    reap.toDelete.exclude(reap.deleted).contains(k),
    s3' = s3.exclude(Set(k)),
    reap' = { ...reap, deleted: reap.deleted.union(Set(k)) },
    live' = live, tomb' = tomb, closures' = closures, keepPush, mark' = mark,
  }

  // DeleteTombstonedObjects; COMMIT.
  action reapCommit: bool = all {
    reap.phase == "deleting",
    tomb' = tomb.exclude(reap.deleted),
    live' = live,
    mark' = touch(reap.deleted),
    reap' = noReap,
    s3' = s3, closures' = closures, keepPush,
  }

  action step = any {
    nondet p = PUSHES.oneOf()
    nondet k = KEYS.oneOf()
    any {
      register(p, k), readBegin(p), readRow(p, k), readAbort(p), readDone(p), s3Put(p, k), register1(p, k), commit(p), clientExit(p), abandon(p),
    },
    nondet k = KEYS.oneOf()
    any { dropClosure(k), markRow(k), reapLock(k), reapS3(k) },
    markBegin, markAbort, markCommit, reapRecheck, reapCommit,
  }

  // ----------------------------------------------------------- properties

  // Everything a committed closure references is in S3.
  val safe = reach(closures).subseteq(s3)

  val KS = KEYS.powerset()

  val typeOK = all {
    KS.contains(s3), KS.contains(live), KS.contains(tomb), KS.contains(closures),
    PUSHES.setOfMaps(Set("idle", "registered", "reading", "uploading", "failed", "gone")).contains(cli),
    PUSHES.setOfMaps(Set(true, false)).contains(pending),
    PUSHES.setOfMaps(KEYS).contains(root),
    PUSHES.setOfMaps(KS).contains(snap), PUSHES.setOfMaps(KS).contains(read), PUSHES.setOfMaps(KS).contains(slock),
    PUSHES.setOfMaps(KS).contains(todo), PUSHES.setOfMaps(KS).contains(putK),
    tuples(Set(true, false), KS, KS, KS).map(((o, s, x, d)) => { on: o, stale: s, xlock: x, dirty: d }).contains(mark),
    tuples(Set("idle", "locking", "deleting"), KS, KS, KS)
      .map(((ph, x, t, d)) => { phase: ph, xlock: x, toDelete: t, deleted: d }).contains(reap),
  }

  val indInv = all {
    typeOK,
    live.intersect(tomb) == Set(),
    live.subseteq(s3),
    reach(closures).subseteq(live),

    // reaper holds only tombstones and deletes only what it holds
    reap.xlock.subseteq(tomb),
    (reap.phase == "idle") == (reap.xlock == Set()),
    reap.phase != "deleting" implies (reap.toDelete == Set() and reap.deleted == Set()),
    reap.toDelete.subseteq(reap.xlock),
    reap.deleted.subseteq(reap.toDelete),

    // mark: rows not written since the snapshot are still as the snapshot
    // saw them; rows it managed to lock were not written
    not(mark.on) implies (mark.stale == Set() and mark.xlock == Set() and mark.dirty == Set()),
    mark.on implies reap.phase == "idle",
    mark.xlock.subseteq(mark.stale),
    mark.xlock.intersect(mark.dirty) == Set(),
    mark.stale.exclude(mark.dirty).subseteq(live),
    mark.stale.exclude(mark.dirty).intersect(reach(closures)) == Set(),

    PUSHES.forall(p => {
      val ps = st(p)
      val ks = reach(Set(ps.root))
      all {
        // shape per client phase
        (ps.cli == "idle") implies (ps == idlePush.with("root", ps.root)),
        Set("registered", "failed", "gone").contains(ps.cli) implies
          (ps.snap == Set() and ps.read == Set() and ps.slock == Set() and ps.todo == Set() and ps.put == Set()),
        ps.cli == "gone" implies ps.pending,
        ps.cli != "reading" implies ps.slock == Set(),
        ps.read.subseteq(ks), ps.todo.subseteq(ks), ps.put.subseteq(ks),
        ps.todo.intersect(ps.put) == Set(),
        // GetLiveObjects: what it reported live is share-locked, hence
        // still live; share locks exclude row writers
        ps.cli == "reading" implies all {
          ps.put == Set(),
          ps.todo.subseteq(ps.read),
          ps.slock.subseteq(ps.read), ps.slock.subseteq(ps.snap), ps.slock.subseteq(rows),
          ps.read.exclude(ps.todo).subseteq(ps.slock),
          ps.read.exclude(ps.todo).subseteq(live),
          ps.slock.intersect(xlocked) == Set(),
        },
        ps.cli == "uploading" implies ps.read == ks,
        // a live pending push never loses what it relies on
        (ps.pending and ps.cli == "reading") implies
          ks.intersect(reap.toDelete).subseteq(ps.snap.exclude(ps.read)),
        (ps.pending and ps.cli == "uploading") implies all {
          ks.intersect(reap.toDelete) == Set(),
          ks.exclude(ps.todo).subseteq(s3),
        },
      }
    }),
  }
}
