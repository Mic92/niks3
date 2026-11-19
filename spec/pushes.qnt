// -*- mode: Bluespec; -*-
// Pushes, uploads and GC racing on the objects table, pending_objects and S3.
// Every step is one statement's snapshot, one commit or one S3 call.
//
// Push:
//   1. Commit a pending row for every object of the closure.
//   2. Look up which objects are live. The lookup waits for rows the sweep
//      holds, and offers the rest for upload.
//   3. Upload, register and commit.
//
// GC:
//   1. Drop old closures.
//   2. Mark, once or twice. The statement decides on its snapshot and writes
//      later: a live object nothing reaches becomes a tombstone, a tombstone
//      nothing reaches is confirmed.
//   3. Sweep confirmed tombstones: select on a snapshot, lock, recheck pending
//      rows, delete from S3, delete the rows. A failed S3 delete keeps the row.
//
// The model has no clock. A push can expire at any time and the sweep has no
// grace period, so safety does not rest on either.
//
// Check with (about 25 million states, several minutes):
//   quint verify --backend tlc --main gc_pushes --invariant safe spec/pushes.qnt
//
// A client that stops is a push that takes no further step. The older
// POST /api/pending_closures, whose commit checks nothing, is not modelled.

module gc_pushes {

  // Tomb: one mark found it unreachable, or its push expired. Dead: a later mark confirmed it.
  type Row = NoRow | Live | Tomb | Dead
  type Obj = { row: Row, refs: Set[str] }
  type Phase = Idle | Inserted | Created | Done | Expired
  type Push = {
    phase: Phase, root: str,
    offered: Set[str],   // keys the server told the client to upload
    uploaded: Set[str],  // keys the client has PUT to S3
  }
  type GcPhase = GIdle | GMark | GMarking | GSweep
  type Gc = {
    phase: GcPhase,
    marks: int,          // marks left in this run
    stale: Set[str],     // what the running mark decided on its snapshot
    cand: Set[str],      // selected by the sweep on a snapshot, not locked yet
    picked: Set[str],    // locked, S3 delete pending
    s3gone: Set[str],    // deleted from S3, DB delete pending
  }

  pure val CLIENTS: Set[str] = Set("c1", "c2")
  pure val KEYS: Set[str] = Set("a", "b", "c")

  pure val REFS: str -> Set[str] = Map(
    "a" -> Set(),
    "b" -> Set("a"),
    "c" -> Set("b"),
  )

  var rows: str -> Obj
  var s3: Set[str]
  var pend: str -> Set[str]
  var pushes: str -> Push
  var closureRoots: Set[str]
  var gc: Gc

  // reflexive-transitive closure; KEYS.size() rounds reach the fixpoint of a DAG
  pure def expand(seed: Set[str], refsOf: str -> Set[str]): Set[str] =
    0.to(KEYS.size()).fold(seed, (acc, _) => acc.union(acc.map(k => refsOf.get(k)).flatten()))

  pure def closureOf(roots: Set[str]): Set[str] = expand(roots, REFS)

  pure val noObj: Obj = { row: NoRow, refs: Set() }
  pure val newPush: Push = { phase: Idle, root: "", offered: Set(), uploaded: Set() }
  pure val idleGc: Gc = { phase: GIdle, marks: 0, stale: Set(), cand: Set(), picked: Set(), s3gone: Set() }

  val pendingKeys: Set[str] = CLIENTS.map(c => pend.get(c)).flatten()

  // rows the sweep holds locked; writers wait, i.e. their action is disabled
  val locked: Set[str] = gc.picked.union(gc.s3gone)

  // Frame helpers: the named variable changes, everything else stays.
  action onlyRows(r: str -> Obj): bool = all {
    rows' = r, s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots, gc' = gc,
  }
  action onlyPushes(p: str -> Push): bool = all {
    pushes' = p, rows' = rows, s3' = s3, pend' = pend, closureRoots' = closureRoots, gc' = gc,
  }
  action onlyGc(g: Gc): bool = all {
    gc' = g, rows' = rows, s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots,
  }

  action init = all {
    rows' = KEYS.mapBy(_ => noObj),
    s3' = Set(),
    pend' = CLIENTS.mapBy(_ => Set()),
    pushes' = CLIENTS.mapBy(_ => newPush),
    closureRoots' = Set(),
    gc' = idleGc,
  }

  // InsertPush and InsertPendingObjects, committed
  action createInsert(c: str, root: str): bool = all {
    pushes.get(c).phase != Inserted,
    pushes.get(c).phase != Created,
    pend' = pend.set(c, closureOf(Set(root))),
    pushes' = pushes.set(c, { ...newPush, phase: Inserted, root: root }),
    rows' = rows, s3' = s3, closureRoots' = closureRoots, gc' = gc,
  }

  // GetExistingObjects; FOR KEY SHARE waits for a sweep holding a row
  action createLookup(c: str): bool = {
    val cl = pend.get(c)
    all {
      pushes.get(c).phase == Inserted,
      not(cl.exists(k => locked.contains(k))),
      onlyPushes(pushes.set(c, { ...pushes.get(c), phase: Created, offered: cl.filter(k => rows.get(k).row != Live) })),
    }
  }

  // PUT to the presigned URL, which outlives the push's cleanup
  action upload(c: str, k: str): bool = all {
    pushes.get(c).phase == Created or pushes.get(c).phase == Expired,
    pushes.get(c).offered.contains(k),
    not(pushes.get(c).uploaded.contains(k)),
    s3' = s3.union(Set(k)),
    pushes' = pushes.set(c, { ...pushes.get(c), uploaded: pushes.get(c).uploaded.union(Set(k)) }),
    rows' = rows, pend' = pend, closureRoots' = closureRoots, gc' = gc,
  }

  // RegisterCompletedObject, only while some push has a pending row for the
  // key. The references come from the push's own row, if it still has one.
  action register(c: str, k: str): bool = all {
    pend.get(c).contains(k),
    pushes.get(c).uploaded.contains(k),
    not(locked.contains(k)),
    onlyRows(rows.set(k, { row: Live, refs: rows.get(k).refs.union(if (pend.get(c).contains(k)) REFS.get(k) else Set()) })),
  }

  // commit_push refuses when a root or a reference of a pending row is
  // neither live nor pending in this push. S3 is not consulted.
  def commitMissing(c: str): bool = {
    val own = pend.get(c)
    val declared = Set(pushes.get(c).root).union(own.map(k => REFS.get(k)).flatten())
    declared.exists(k => rows.get(k).row != Live and not(own.contains(k)))
  }

  action commit(c: str): bool = all {
    pushes.get(c).phase == Created,
    pushes.get(c).offered.subseteq(pushes.get(c).uploaded),
    not(commitMissing(c)),
    not(pend.get(c).exists(k => locked.contains(k))),
    rows' = KEYS.mapBy(k =>
      if (pend.get(c).contains(k))
        { row: Live, refs: if (rows.get(k).row == Live) rows.get(k).refs.union(REFS.get(k)) else REFS.get(k) }
      else rows.get(k)),
    pend' = pend.set(c, Set()),
    pushes' = pushes.set(c, { ...pushes.get(c), phase: Done }),
    closureRoots' = closureRoots.union(Set(pushes.get(c).root)),
    s3' = s3, gc' = gc,
  }

  // CleanupPendingClosures: drops a push and tombstones its objects that have no row
  action cleanup(c: str): bool = all {
    pushes.get(c).phase == Inserted or pushes.get(c).phase == Created,
    rows' = KEYS.mapBy(k =>
      if (rows.get(k).row == NoRow and pend.get(c).contains(k)) { row: Tomb, refs: REFS.get(k) }
      else rows.get(k)),
    pend' = pend.set(c, Set()),
    pushes' = pushes.set(c, { ...pushes.get(c), phase: Expired }),
    s3' = s3, closureRoots' = closureRoots, gc' = gc,
  }

  // cleanupClosureOlderThan (pins ignored); a forced run marks twice
  action gcStart(dropped: Set[str], marks: int): bool = all {
    gc.phase == GIdle,
    dropped.subseteq(closureRoots),
    closureRoots' = closureRoots.exclude(dropped),
    gc' = { ...idleGc, phase: GMark, marks: marks },
    rows' = rows, s3' = s3, pend' = pend, pushes' = pushes,
  }

  // MarkStaleObjects takes its snapshot. Reachability starts from closures
  // and pending rows and walks tombstones too.
  action gcMarkSnap: bool = {
    val reach = expand(closureRoots.union(pendingKeys).filter(k => rows.get(k).row != NoRow), KEYS.mapBy(k => rows.get(k).refs))
    all {
      gc.phase == GMark,
      onlyGc({ ...gc, phase: GMarking, stale: KEYS.filter(k =>
        (rows.get(k).row == Live or rows.get(k).row == Tomb) and not(reach.contains(k)) and not(pendingKeys.contains(k))) }),
    }
  }

  // MarkStaleObjects writes. A row that changed since the snapshot is judged
  // by its state now; whether it is reachable is not looked at again.
  action gcMarkWrite: bool = all {
    gc.phase == GMarking,
    rows' = KEYS.mapBy(k =>
      if (not(gc.stale.contains(k))) rows.get(k)
      else if (rows.get(k).row == Live) { ...rows.get(k), row: Tomb }
      else if (rows.get(k).row == Tomb) { ...rows.get(k), row: Dead }
      else rows.get(k)),
    gc' = { ...gc, phase: if (gc.marks > 1) GMark else GSweep, marks: gc.marks - 1, stale: Set() },
    s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots,
  }

  // Step 1 of LockObjectsReadyForDeletion: the query's snapshot picks candidates.
  // A push can insert its pending rows after this.
  action sweepSnap(k: str): bool = all {
    gc.phase == GSweep,
    rows.get(k).row == Dead,
    not(pendingKeys.contains(k)),
    not(locked.contains(k)),
    onlyGc({ ...gc, cand: gc.cand.union(Set(k)) }),
  }

  // Step 2: FOR UPDATE SKIP LOCKED, then the recheck of pending rows on a fresh
  // snapshot. Without the recheck a push that inserted in between loses its object.
  action sweepLock(k: str): bool = all {
    gc.phase == GSweep,
    gc.cand.contains(k),
    rows.get(k).row == Dead,
    not(locked.contains(k)),
    onlyGc({ ...gc, cand: gc.cand.exclude(Set(k)),
                    picked: if (pendingKeys.contains(k)) gc.picked else gc.picked.union(Set(k)) }),
  }

  // RemoveObjectsWithResult; NoSuchKey is success, failure keeps the row
  action sweepS3(k: str, ok: bool): bool = all {
    gc.picked.contains(k),
    if (ok) all {
      s3' = s3.exclude(Set(k)),
      gc' = { ...gc, picked: gc.picked.exclude(Set(k)), s3gone: gc.s3gone.union(Set(k)) },
    } else all {
      s3' = s3,
      gc' = { ...gc, picked: gc.picked.exclude(Set(k)) },
    },
    rows' = rows, pend' = pend, pushes' = pushes, closureRoots' = closureRoots,
  }

  // DeleteObjects
  action sweepDb(k: str): bool = all {
    gc.s3gone.contains(k),
    rows' = rows.set(k, noObj),
    gc' = { ...gc, s3gone: gc.s3gone.exclude(Set(k)) },
    s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots,
  }

  action gcFinish: bool = all {
    gc.phase == GSweep,
    gc.picked == Set(), gc.s3gone == Set(),
    onlyGc(idleGc),
  }

  // process death or cancelled context: the work lists are lost, locks released
  action gcAbort: bool = all {
    gc.phase != GIdle,
    onlyGc(idleGc),
  }

  action step = any {
    nondet c = CLIENTS.oneOf() nondet root = KEYS.oneOf() createInsert(c, root),
    nondet c = CLIENTS.oneOf() createLookup(c),
    nondet c = CLIENTS.oneOf() nondet k = KEYS.oneOf() upload(c, k),
    nondet c = CLIENTS.oneOf() nondet k = KEYS.oneOf() register(c, k),
    nondet c = CLIENTS.oneOf() commit(c),
    nondet c = CLIENTS.oneOf() cleanup(c),
    nondet d = closureRoots.powerset().oneOf() nondet m = Set(1, 2).oneOf() gcStart(d, m),
    gcMarkSnap,
    gcMarkWrite,
    nondet k = KEYS.oneOf() sweepSnap(k),
    nondet k = KEYS.oneOf() sweepLock(k),
    nondet k = KEYS.oneOf() nondet ok = Set(true, false).oneOf() sweepS3(k, ok),
    nondet k = KEYS.oneOf() sweepDb(k),
    gcFinish,
    gcAbort,
  }

  // What a closure reaches can be fetched. A tombstone there keeps its file:
  // the next mark reaches it and does not confirm it.
  val closureComplete: bool =
    closureOf(closureRoots).forall(k => (rows.get(k).row == Live or rows.get(k).row == Tomb) and s3.contains(k))

  val liveBacked: bool = KEYS.forall(k => rows.get(k).row != Live or s3.contains(k))

  val safe: bool = closureComplete and liveBacked
}
