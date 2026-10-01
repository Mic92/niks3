// -*- mode: Bluespec; -*-
// Pushes, uploads and GC racing on the objects table, pending_objects and S3.
// Every step is one transaction or one S3 call; GC steps interleave with pushes.
// Modelled server: every closure object gets a pending row, the sweep skips
// pending keys, row-locks what it picked and keeps the tombstone when the S3
// delete fails. COMMIT_CHECK selects commit_push (new API) over
// commit_pending_closure (old API).

module gc_pushes {

  const COMMIT_CHECK: bool

  type Row = NoRow | Live | Tomb
  type Obj = { row: Row, first: int, refs: Set[str] }
  type Phase = Idle | Creating | Created | Done | Expired
  type Push = {
    phase: Phase, alive: bool, roots: Set[str], at: int,
    offered: Set[str],   // keys the server told the client to upload
    uploaded: Set[str],  // keys the client has PUT to S3
  }
  type GcPhase = GIdle | GCleanup | GClosures | GMark | GSweep
  type Gc = {
    phase: GcPhase, grace: int,
    picked: Set[str],    // returned by GetObjectsReadyForDeletion, S3 delete pending
    s3gone: Set[str],    // deleted from S3, DB delete pending
  }

  pure val CLIENTS: Set[str] = Set("c1", "c2")
  pure val KEYS: Set[str] = Set("a", "b", "c", "d")
  pure val MAXT: int = 6
  pure val PENDING_AGE: int = 2

  pure val REFS: str -> Set[str] = Map(
    "a" -> Set(),
    "b" -> Set("a"),
    "c" -> Set("a"),
    "d" -> Set("b", "c"),
  )

  var rows: str -> Obj
  var s3: Set[str]
  var pend: str -> Set[str]
  var pushes: str -> Push
  var closureRoots: Set[str]
  var gc: Gc
  var now: int

  // reflexive-transitive closure; KEYS.size() rounds reach the fixpoint of a DAG
  pure def expand(seed: Set[str], refsOf: str -> Set[str]): Set[str] =
    0.to(KEYS.size()).fold(seed, (acc, _) => acc.union(acc.map(k => refsOf.get(k)).flatten()))

  pure def closureOf(roots: Set[str]): Set[str] = expand(roots, REFS)

  pure val noObj: Obj = { row: NoRow, first: -1, refs: Set() }
  pure val newPush: Push = { phase: Idle, alive: true, roots: Set(), at: 0, offered: Set(), uploaded: Set() }
  pure val idleGc: Gc = { phase: GIdle, grace: 0, picked: Set(), s3gone: Set() }

  val pendingKeys: Set[str] = CLIENTS.map(c => pend.get(c)).flatten()

  // rows the sweep holds locked; writers wait, i.e. their action is disabled
  val locked: Set[str] = gc.picked.union(gc.s3gone)

  // rows a running create transaction holds FOR KEY SHARE; the sweep skips them
  val createLocked: Set[str] =
    CLIENTS.filter(c => pushes.get(c).phase == Creating and pushes.get(c).alive)
      .map(c => closureOf(pushes.get(c).roots)).flatten()

  action init = all {
    rows' = KEYS.mapBy(_ => noObj),
    s3' = Set(),
    pend' = CLIENTS.mapBy(_ => Set()),
    pushes' = CLIENTS.mapBy(_ => newPush),
    closureRoots' = Set(),
    gc' = idleGc,
    now' = 0,
  }

  action tick = all {
    now < MAXT,
    now' = now + 1,
    rows' = rows, s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots, gc' = gc,
  }

  // read half of createPendingClosureInner; started_at is taken here
  action createBegin(c: str, roots: Set[str], verifyS3: bool): bool = {
    val cl = closureOf(roots)
    val present = cl.filter(k => rows.get(k).row == Live and (not(verifyS3) or s3.contains(k)))
    all {
      pushes.get(c).phase != Creating,
      pushes.get(c).phase != Created,
      pushes.get(c).alive,
      roots != Set(),
      not(cl.exists(k => locked.contains(k))),
      pushes' = pushes.set(c, { phase: Creating, alive: true, roots: roots, at: now,
                                offered: cl.exclude(present), uploaded: Set() }),
      rows' = rows, s3' = s3, pend' = pend, closureRoots' = closureRoots, gc' = gc, now' = now,
    }
  }

  // InsertPendingObjects and commit of the create transaction
  action createFinish(c: str): bool = all {
    pushes.get(c).phase == Creating,
    pushes.get(c).alive,
    pend' = pend.set(c, closureOf(pushes.get(c).roots)),
    pushes' = pushes.set(c, { ...pushes.get(c), phase: Created }),
    rows' = rows, s3' = s3, closureRoots' = closureRoots, gc' = gc, now' = now,
  }

  // the presigned URL outlives the push's cleanup
  action upload(c: str, k: str): bool = all {
    pushes.get(c).alive,
    pushes.get(c).phase == Created or pushes.get(c).phase == Expired,
    pushes.get(c).offered.contains(k),
    not(pushes.get(c).uploaded.contains(k)),
    s3' = s3.union(Set(k)),
    pushes' = pushes.set(c, { ...pushes.get(c), uploaded: pushes.get(c).uploaded.union(Set(k)) }),
    rows' = rows, pend' = pend, closureRoots' = closureRoots, gc' = gc, now' = now,
  }

  // RegisterCompletedObject, only while the push's own pending row exists
  action register(c: str, k: str): bool = all {
    pend.get(c).contains(k),
    pushes.get(c).uploaded.contains(k),
    not(locked.contains(k)),
    rows' = rows.set(k, { row: Live, first: -1, refs: rows.get(k).refs.union(REFS.get(k)) }),
    s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots, gc' = gc, now' = now,
  }

  // commit_push: what the push names is live or pending; S3 is not consulted
  def commitMissing(c: str): bool = {
    val own = pend.get(c)
    val declared = pushes.get(c).roots.union(own.map(k => REFS.get(k)).flatten())
    declared.exists(k => rows.get(k).row != Live and not(own.contains(k)))
  }

  action commit(c: str): bool = all {
    pushes.get(c).phase == Created,
    pushes.get(c).alive,
    pushes.get(c).offered.subseteq(pushes.get(c).uploaded),
    not(COMMIT_CHECK and commitMissing(c)),
    not(pend.get(c).exists(k => locked.contains(k))),
    rows' = KEYS.mapBy(k =>
      if (pend.get(c).contains(k)) { row: Live, first: -1, refs: rows.get(k).refs.union(REFS.get(k)) }
      else rows.get(k)),
    pend' = pend.set(c, Set()),
    pushes' = pushes.set(c, { ...pushes.get(c), phase: Done }),
    closureRoots' = closureRoots.union(pushes.get(c).roots),
    s3' = s3, gc' = gc, now' = now,
  }

  action crash(c: str): bool = all {
    pushes.get(c).alive,
    pushes' = pushes.set(c, { ...pushes.get(c), alive: false }),
    rows' = rows, s3' = s3, pend' = pend, closureRoots' = closureRoots, gc' = gc, now' = now,
  }

  // CleanupPendingClosures; tombstones are dated at the cutoff, so sweepable at once
  def oldPushes: Set[str] =
    CLIENTS.filter(c => pushes.get(c).phase == Created and now - pushes.get(c).at >= PENDING_AGE)

  action cleanup(nextGc: Gc): bool = all {
    rows' = KEYS.mapBy(k =>
      if (rows.get(k).row == NoRow and oldPushes.exists(c => pend.get(c).contains(k)))
        { row: Tomb, first: now - PENDING_AGE, refs: REFS.get(k) }
      else rows.get(k)),
    pend' = CLIENTS.mapBy(c => if (oldPushes.contains(c)) Set() else pend.get(c)),
    pushes' = CLIENTS.mapBy(c =>
      if (oldPushes.contains(c)) { ...pushes.get(c), phase: Expired } else pushes.get(c)),
    s3' = s3, closureRoots' = closureRoots, gc' = nextGc, now' = now,
  }

  // cleanup outside a GC run
  action cleanupStandalone: bool = all { oldPushes != Set(), cleanup(gc) }

  action gcStart(force: bool): bool = all {
    gc.phase == GIdle,
    gc' = { ...idleGc, phase: GCleanup, grace: if (force) 0 else PENDING_AGE },
    rows' = rows, s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots, now' = now,
  }

  action gcCleanup: bool = all { gc.phase == GCleanup, cleanup({ ...gc, phase: GClosures }) }

  // cleanupClosureOlderThan (pins ignored)
  action gcClosures(dropped: Set[str]): bool = all {
    gc.phase == GClosures,
    dropped.subseteq(closureRoots),
    closureRoots' = closureRoots.exclude(dropped),
    gc' = { ...gc, phase: GMark },
    rows' = rows, s3' = s3, pend' = pend, pushes' = pushes, now' = now,
  }

  // MarkStaleObjects, one statement; reachability walks tombstones too
  action gcMark: bool = {
    val reach = expand(closureRoots.filter(k => rows.get(k).row != NoRow), KEYS.mapBy(k => rows.get(k).refs))
    all {
      gc.phase == GMark,
      rows' = KEYS.mapBy(k =>
        if (rows.get(k).row == Live and not(reach.contains(k)) and not(pendingKeys.contains(k)))
          { ...rows.get(k), row: Tomb, first: if (rows.get(k).first >= 0) rows.get(k).first else now }
        else rows.get(k)),
      gc' = { ...gc, phase: GSweep },
      s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots, now' = now,
    }
  }

  // GetObjectsReadyForDeletion
  action sweepPick(k: str): bool = all {
    gc.phase == GSweep,
    rows.get(k).row == Tomb,
    rows.get(k).first >= 0,
    now - rows.get(k).first >= gc.grace,
    not(pendingKeys.contains(k)),
    not(locked.contains(k)),
    not(createLocked.contains(k)),
    gc' = { ...gc, picked: gc.picked.union(Set(k)) },
    rows' = rows, s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots, now' = now,
  }

  // RemoveObjectsWithResult; NoSuchKey is success, failure keeps the tombstone
  action sweepS3(k: str, ok: bool): bool = all {
    gc.picked.contains(k),
    if (ok) all {
      s3' = s3.exclude(Set(k)),
      gc' = { ...gc, picked: gc.picked.exclude(Set(k)), s3gone: gc.s3gone.union(Set(k)) },
    } else all {
      s3' = s3,
      gc' = { ...gc, picked: gc.picked.exclude(Set(k)) },
    },
    rows' = rows, pend' = pend, pushes' = pushes, closureRoots' = closureRoots, now' = now,
  }

  // DeleteObjects, by key regardless of the row's state
  action sweepDb(k: str): bool = all {
    gc.s3gone.contains(k),
    rows' = rows.set(k, noObj),
    gc' = { ...gc, s3gone: gc.s3gone.exclude(Set(k)) },
    s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots, now' = now,
  }

  action gcFinish: bool = all {
    gc.phase == GSweep,
    gc.picked == Set(), gc.s3gone == Set(),
    gc' = idleGc,
    rows' = rows, s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots, now' = now,
  }

  // process death or cancelled context: work lists are lost, locks released
  action gcAbort: bool = all {
    gc.phase != GIdle,
    gc' = idleGc,
    rows' = rows, s3' = s3, pend' = pend, pushes' = pushes, closureRoots' = closureRoots, now' = now,
  }

  action step = any {
    tick,
    nondet c = CLIENTS.oneOf() nondet roots = KEYS.powerset().oneOf() nondet v = Set(true, false).oneOf()
      createBegin(c, roots, v),
    nondet c = CLIENTS.oneOf() createFinish(c),
    nondet c = CLIENTS.oneOf() nondet k = KEYS.oneOf() upload(c, k),
    nondet c = CLIENTS.oneOf() nondet k = KEYS.oneOf() register(c, k),
    nondet c = CLIENTS.oneOf() commit(c),
    nondet c = CLIENTS.oneOf() crash(c),
    cleanupStandalone,
    nondet f = Set(true, false).oneOf() gcStart(f),
    gcCleanup,
    nondet d = closureRoots.powerset().oneOf() gcClosures(d),
    gcMark,
    nondet k = KEYS.oneOf() sweepPick(k),
    nondet k = KEYS.oneOf() nondet ok = Set(true, false).oneOf() sweepS3(k, ok),
    nondet k = KEYS.oneOf() sweepDb(k),
    gcFinish,
    gcAbort,
  }

  val closureComplete: bool =
    closureOf(closureRoots).forall(k => rows.get(k).row == Live and s3.contains(k))

  val liveBacked: bool = KEYS.forall(k => rows.get(k).row != Live or s3.contains(k))

  val safe: bool = closureComplete and liveBacked
}

// POST /api/pushes
module new_api {
  import gc_pushes(COMMIT_CHECK = true).*
}

// POST /api/pending_closures
module old_api {
  import gc_pushes(COMMIT_CHECK = false).*
}
