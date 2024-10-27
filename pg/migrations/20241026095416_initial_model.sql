-- Basic structure of a binary cache:
--   ././
--  ├──  26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo
--  ├──  4hcdxyjf9yiq7qf3i4548drb6sjmwa1v.narinfo
--  ├──  jwsdpq2yxw43ixalh93z726czz7bay2j.narinfo
--  ├──  log/
--  ├──  nar/
--  │   ├──  08242al70hn299yh1vk6il2cyahh6p86qvm72rmqz1z07q36vsk2.nar.xz
--  │   ├──  1767a9kz9xjpy5nh94d1prn3wv8rlcw7k9xhcsm0qcnx4l5qhq2n.nar.xz
--  │   ├──  17fm917985vcvrkrsckjb3i7q6rsxc4xlw8m1d6i5hdmxf9rxhh2.nar.xz
--  │   ├──  1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.xz
--  │   └──  1qva1j5l6gwjlj2xw69r3w8ldcgs14vp33hl7rm124r6q3fw13il.nar.xz
--  ├──  nix-cache-info
--  ├──  realisations/
--  │   └──  sha256:9d7d12c511042dac015ce38181f045b86da5a8d83a6d0364fa3b3fc48d28c203!out.doi
--  ├──  sl141d1g77wvhr050ah87lcyz2czdxa3.narinfo
--  └──  w19cxz37j5nrkg8w80y91bga89310jgi.narinfo
--
-- +goose Up
-- +goose StatementBegin

-- closures act as gcroots for our binary cache
CREATE TABLE closures
(
    key   varchar(1024) primary key,
    updated_at timestamp not null
);

-- objects are the actual files in the the s3 bucket (narinfo, nar, log, etc)
CREATE TABLE objects
(
    key        varchar(1024) primary key
);

-- closure_objects is a many-to-many relationship between closures and objects
CREATE TABLE IF NOT EXISTS closure_objects
(
    closure_key varchar(1024) not null references closures (key),
    object_key  varchar(1024) not null references objects (key)
);

CREATE INDEX closure_objects_closure_key_idx ON closure_objects (closure_key);
CREATE INDEX closure_objects_object_key_idx ON closure_objects (object_key);

-- This is where track not yet uploaded closures
CREATE TABLE pending_closures
(
    id          bigint generated always as identity primary key,
    key         varchar(1024) not null,
    started_at  timestamp not null
);

-- This is where track not yet uploaded objects associated with a pending closure
CREATE TABLE pending_objects
(
    pending_closure_id bigint not null references pending_closures (id),
    key        varchar(1024) primary key
);
CREATE INDEX pending_objects_pending_closure_id_idx ON pending_objects (pending_closure_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP INDEX closure_objects_closure_key_idx;
DROP INDEX pending_objects_pending_closure_id_idx;

DROP TABLE closures;
DROP TABLE objects;
DROP TABLE closure_objects;
DROP TABLE pending_closures;
DROP TABLE pending_objects;
-- +goose StatementEnd
