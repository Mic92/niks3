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
    key varchar(1024) PRIMARY KEY,
    updated_at timestamp NOT NULL
);
CREATE INDEX closures_updated_at_idx ON closures (updated_at);

-- objects are the actual files in the the s3 bucket (narinfo, nar, log, etc)
CREATE TABLE objects
(
    key varchar(1024) PRIMARY KEY,
    refs varchar(1024) [] NOT NULL DEFAULT '{}', -- Direct references to other objects (not transitive)
    deleted_at timestamp
);

-- Create GIN index for efficient reference lookups
CREATE INDEX objects_refs_gin ON objects USING gin (refs);

-- This is where track not yet uploaded closures
CREATE TABLE pending_closures
(
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    key varchar(1024) NOT NULL,
    started_at timestamp NOT NULL
);
CREATE INDEX pending_closures_started_at_idx ON pending_closures (started_at);

-- This is where track not yet uploaded objects associated with a pending closure
CREATE TABLE pending_objects
(
    pending_closure_id bigint NOT NULL REFERENCES pending_closures (id) ON DELETE CASCADE,
    key varchar(1024) NOT NULL,
    refs varchar(1024) [] NOT NULL DEFAULT '{}', -- Direct references to other objects
    PRIMARY KEY (key, pending_closure_id)
);
CREATE INDEX pending_objects_pending_closure_id_idx ON pending_objects (
    pending_closure_id
);

-- Track multipart uploads for cleanup
CREATE TABLE multipart_uploads (
    pending_closure_id bigint NOT NULL REFERENCES pending_closures (id) ON DELETE CASCADE,
    object_key varchar(1024) NOT NULL,
    upload_id varchar(1024) NOT NULL,
    PRIMARY KEY (pending_closure_id, object_key)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP INDEX objects_refs_gin;
DROP INDEX pending_objects_pending_closure_id_idx;
DROP INDEX closures_updated_at_idx;
DROP INDEX pending_closures_started_at_idx;

DROP TABLE multipart_uploads;
DROP TABLE pending_objects;
DROP TABLE pending_closures;
DROP TABLE objects;
DROP TABLE closures;
-- +goose StatementEnd
