{
  pkgs,
  treefmt-nix,
}:
let
  treefmtEval = treefmt-nix.lib.evalModule pkgs {
    projectRootFile = ".git/config";
    settings.global.excludes = [ ".envrc" ];
    programs.nixfmt.enable = true;
    programs.deadnix.enable = true;
    programs.gofumpt.enable = true;
    programs.yamlfmt.enable = true;
    programs.mdformat.enable = true;
    programs.sqlfluff.enable = true;
    programs.sqlfluff.dialect = "postgres";
    programs.sqlfluff.excludes = [ "server/pg/query.sql" ];
    programs.rustfmt.enable = true;
    programs.rustfmt.edition = "2021";
  };
in
treefmtEval.config.build.wrapper
