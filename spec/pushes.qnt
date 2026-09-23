// -*- mode: Bluespec; -*-
// POST /api/pushes racing with garbage collection.
// A key has a DB row (objects table), an S3 object and at most one pending
// owner (pending_objects). Objects are abstract keys with a fixed ref graph.
// The clock is coarse: PENDING_AGE is how long a push may stay pending,
// GRACE how long a tombstone lives before the purge deletes it.

module pushes {

  type Row = NoRow | Live | Tomb
  type Obj = { row: Row, since: int, s3: bool, pend: str }
  type Phase = Idle | Created | Done | Expired
  type Client = { phase: Phase, alive: bool, roots: Set[str], at: int }

  pure val CLIENTS: Set[str] = Set("c1", "c2")
  pure val KEYS: Set[str] = Set("a", "b", "c", "d")
  pure val MAXT: int = 5
  pure val PENDING_AGE: int = 2
  pure val GRACE: int = 2

  pure val REFS: str -> Set[str] = Map(
    "a" -> Set(),
    "b" -> Set("a"),
    "c" -> Set("a"),
    "d" -> Set("b", "c"),
  )

  var objs: str -> Obj
  var clients: str -> Client
  var closureRoots: Set[str]   // rows of `closures`
  var now: int
  var puts: str -> int         // S3 uploads per key
  var purges: str -> int       // S3 deletes per key

  // Reflexive-transitive closure of REFS, written out so no fold is needed.
  pure val REACH: str -> Set[str] = Map(
    "a" -> Set("a"),
    "b" -> Set("a", "b"),
    "c" -> Set("a", "c"),
    "d" -> Set("a", "b", "c", "d"),
  )
  pure def closureOf(s: Set[str]): Set[str] = KEYS.filter(k => s.exists(r => REACH.get(r).contains(k)))

  pure val noObj: Obj = { row: NoRow, since: 0, s3: false, pend: "" }

  action init = all {
    objs' = KEYS.mapBy(_ => noObj),
    clients' = CLIENTS.mapBy(_ => { phase: Idle, alive: true, roots: Set(), at: 0 }),
    closureRoots' = Set(),
    now' = 0,
    puts' = KEYS.mapBy(_ => 0),
    purges' = KEYS.mapBy(_ => 0),
  }

  action tick = all {
    now < MAXT,
    now' = now + 1,
    objs' = objs, clients' = clients, closureRoots' = closureRoots, puts' = puts, purges' = purges,
  }

  // A pending owner that crashed or expired no longer holds its key.
  def ownerGone(k: str): bool =
    objs.get(k).pend == "" or clients.get(objs.get(k).pend).phase != Created or not(clients.get(objs.get(k).pend).alive)

  // One transaction: register the push and claim every key under the roots
  // that is not a live row. Keys another client is still uploading stay theirs.
  action create(c: str, roots: Set[str]): bool = all {
    clients.get(c).phase == Idle,
    clients.get(c).alive,
    roots != Set(),
    clients' = clients.set(c, { phase: Created, alive: true, roots: roots, at: now }),
    objs' = objs.keys().mapBy(k => {
      val o = objs.get(k)
      if (closureOf(roots).contains(k) and o.row != Live and ownerGone(k)) { ...o, pend: c } else o
    }),
    closureRoots' = closureRoots, now' = now, puts' = puts, purges' = purges,
  }

  action upload(c: str, k: str): bool = all {
    clients.get(c).alive,
    clients.get(c).phase == Created,
    objs.get(k).pend == c,
    not(objs.get(k).s3),
    objs' = objs.set(k, { ...objs.get(k), s3: true }),
    puts' = puts.set(k, puts.get(k) + 1),
    clients' = clients, closureRoots' = closureRoots, now' = now, purges' = purges,
  }

  // Everything under the roots is a live row or uploaded by this push. The
  // check runs in the commit transaction, so a key that was live at create
  // time and got collected since makes the commit fail.
  action commit(c: str): bool = all {
    clients.get(c).phase == Created,
    clients.get(c).alive,
    closureOf(clients.get(c).roots).forall(k =>
      objs.get(k).row == Live or (objs.get(k).pend == c and objs.get(k).s3)),
    objs' = objs.keys().mapBy(k =>
      if (objs.get(k).pend == c) { row: Live, since: 0, s3: objs.get(k).s3, pend: "" } else objs.get(k)),
    closureRoots' = closureRoots.union(clients.get(c).roots),
    clients' = clients.set(c, { ...clients.get(c), phase: Done }),
    now' = now, puts' = puts, purges' = purges,
  }

  action crash(c: str): bool = all {
    clients.get(c).alive,
    clients' = clients.set(c, { ...clients.get(c), alive: false }),
    objs' = objs, closureRoots' = closureRoots, now' = now, puts' = puts, purges' = purges,
  }

  // CleanupPendingClosures: pending rows of an old push become tombstones
  // unless the object already has a row.
  action cleanupPending(c: str): bool = all {
    clients.get(c).phase == Created,
    now - clients.get(c).at >= PENDING_AGE,
    clients' = clients.set(c, { ...clients.get(c), phase: Expired }),
    objs' = objs.keys().mapBy(k => {
      val o = objs.get(k)
      if (o.pend == c) { row: if (o.row == NoRow) Tomb else o.row, since: if (o.row == NoRow) now else o.since, s3: o.s3, pend: "" }
      else o
    }),
    closureRoots' = closureRoots, now' = now, puts' = puts, purges' = purges,
  }

  // cleanupClosureOlderThan
  action dropClosure(r: str): bool = all {
    closureRoots.contains(r),
    closureRoots' = closureRoots.exclude(Set(r)),
    objs' = objs, clients' = clients, now' = now, puts' = puts, purges' = purges,
  }

  // Every GC run starts with CleanupPendingClosures, so mark and purge only
  // happen once no push is older than PENDING_AGE.
  val pendingCleaned: bool =
    CLIENTS.forall(c => clients.get(c).phase != Created or now - clients.get(c).at < PENDING_AGE)

  // MarkStaleObjects: not reachable from a closure and no pending row
  action markStale(k: str): bool = all {
    pendingCleaned,
    objs.get(k).row == Live,
    not(closureOf(closureRoots).contains(k)),
    objs.get(k).pend == "",
    objs' = objs.set(k, { ...objs.get(k), row: Tomb, since: now }),
    clients' = clients, closureRoots' = closureRoots, now' = now, puts' = puts, purges' = purges,
  }

  // GetObjectsReadyForDeletion + RemoveObjects + DeleteObjects
  action purge(k: str): bool = all {
    pendingCleaned,
    objs.get(k).row == Tomb,
    now - objs.get(k).since >= GRACE,
    objs' = objs.set(k, { ...objs.get(k), row: NoRow, s3: false }),
    purges' = purges.set(k, purges.get(k) + 1),
    clients' = clients, closureRoots' = closureRoots, now' = now, puts' = puts,
  }

  action step = any {
    tick,
    nondet c = CLIENTS.oneOf() nondet roots = KEYS.powerset().oneOf() create(c, roots),
    nondet c = CLIENTS.oneOf() nondet k = KEYS.oneOf() upload(c, k),
    nondet c = CLIENTS.oneOf() commit(c),
    nondet c = CLIENTS.oneOf() crash(c),
    nondet c = CLIENTS.oneOf() cleanupPending(c),
    nondet r = KEYS.oneOf() dropClosure(r),
    nondet k = KEYS.oneOf() markStale(k),
    nondet k = KEYS.oneOf() purge(k),
  }

  // every closure row has all its objects in the DB and in S3
  val closureComplete: bool =
    closureOf(closureRoots).forall(k => objs.get(k).row == Live and objs.get(k).s3)

  val noDuplicateUploads: bool =
    KEYS.forall(k => puts.get(k) <= 1 + purges.get(k))

  val safe: bool = closureComplete and noDuplicateUploads

  // Inductive strengthening of `safe`. The clock plays no part: the commit
  // re-check alone keeps closures complete.
  // Membership form: Apalache needs every variable constrained by `in` to
  // start the inductive step from an arbitrary state.
  pure val ROWS: Set[Row] = Set(NoRow, Live, Tomb)
  pure val TIMES: Set[int] = 0.to(MAXT)
  pure val PENDS: Set[str] = CLIENTS.union(Set(""))
  pure val OBJS: Set[Obj] =
    ROWS.map(r => TIMES.map(t => Set(true, false).map(b => PENDS.map(p => { row: r, since: t, s3: b, pend: p }))))
        .flatten().flatten().flatten()
  pure val PHASES: Set[Phase] = Set(Idle, Created, Done, Expired)
  pure val CLS: Set[Client] =
    PHASES.map(ph => Set(true, false).map(al => KEYS.powerset().map(r => TIMES.map(t =>
      { phase: ph, alive: al, roots: r, at: t }))))
      .flatten().flatten().flatten()

  val typeOk: bool = all {
    objs.in(KEYS.setOfMaps(OBJS)),
    clients.in(CLIENTS.setOfMaps(CLS)),
    closureRoots.in(KEYS.powerset()),
    puts.in(KEYS.setOfMaps(Nat)),
    purges.in(KEYS.setOfMaps(Nat)),
    now.in(TIMES),
  }

  // a pending key belongs to a running push and lies under its roots
  val pendOk: bool = KEYS.forall(k => {
    val p = objs.get(k).pend
    p == "" or (clients.get(p).phase == Created and closureOf(clients.get(p).roots).contains(k))
  })

  val liveHasS3: bool = KEYS.forall(k => objs.get(k).row != Live or objs.get(k).s3)

  // puts and purges pair up: an upload needs s3 to be false, a purge clears it
  val putsBound: bool =
    KEYS.forall(k => puts.get(k) <= purges.get(k) + (if (objs.get(k).s3) 1 else 0))

  val inv: bool = typeOk and pendOk and liveHasS3 and putsBound and closureComplete
}
