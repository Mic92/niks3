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
CREATE TABLE closures
(
    key   varchar(1024) primary key,
    updated_at timestamp not null
);

CREATE TABLE uploads
(
    id          bigint generated always as identity primary key,
    started_at  timestamp not null,
    closure_key varchar(1024)  not null references closures (key)
);

CREATE TABLE objects
(
    key        varchar(1024) primary key,
    reference_count integer not null
);

-- partial index to find objects with reference_count == 0
CREATE INDEX objects_reference_count_zero_idx ON objects (key) WHERE reference_count = 0;

CREATE TABLE IF NOT EXISTS closure_objects
(
    closure_key varchar(1024) not null references closures (key),
    object_key  varchar(1024) not null references objects (key)
);

CREATE INDEX closure_objects_closure_key_idx ON closure_objects (closure_key);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP INDEX closure_objects_closure_key_idx;
DROP TABLE closure_objects;
DROP INDEX objects_reference_count_zero_idx;
DROP TABLE objects;
DROP TABLE uploads;
DROP TABLE closures;

-- +goose StatementEnd
