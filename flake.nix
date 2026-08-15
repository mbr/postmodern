{
  inputs = {
    nixpkgs.url = "nixpkgs/nixos-26.05";
    fenix = {
      url = "fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "flake-utils";
    pgdb = {
      url = "github:mbr/pgdb-rs";
      inputs.fenix.follows = "fenix";
      inputs.flake-utils.follows = "flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      flake-utils,
      pgdb,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        buildToolchain = fenix.packages.${system}.stable.minimalToolchain;
        devToolchain = fenix.packages.${system}.stable.withComponents [
          "cargo"
          "clippy"
          "rust-analyzer"
          "rust-src"
          "rustc"
          "rustfmt"
        ];

        platform = pkgs.makeRustPlatform {
          cargo = buildToolchain;
          rustc = buildToolchain;
        };

        cargoToml = pkgs.lib.importTOML ./Cargo.toml;

        rustEnv = {
          RUSTFLAGS =
            pkgs.lib.optionalString pkgs.stdenv.isLinux "-Clink-self-contained=-linker "
            # Avoid runtime references from embedded toolchain source paths.
            + "--remap-path-prefix=${buildToolchain}=/rustc";
          OPENSSL_NO_VENDOR = "1";
        };
      in
      {
        packages.default = platform.buildRustPackage (
          rustEnv
          // rec {
            pname = "pm";
            version = cargoToml.workspace.package.version;
            description = "Postgres-backed job queue with transaction-based locking";
            nativeBuildInputs = with pkgs; [
              llvmPackages.bintools
              postgresql
            ];

            src = pkgs.lib.cleanSource ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            meta.mainProgram = pname;
          }
        );

        devShells.default = pkgs.mkShell (
          rustEnv
          // {
            inputsFrom = [ self.packages.${system}.default ];
            packages = [ devToolchain ];
            buildInputs = [
              pkgs.cargo-insta
              pkgs.nixfmt
              pkgs.sqlx-cli
              pgdb.packages.${system}.default
            ];
            RUST_LOG = "debug";
          }
        );
      }
    )
    // {
      nixosModules.default =
        {
          config,
          lib,
          pkgs,
          ...
        }:
        let
          cfg = config.services.postmodern;
        in
        {
          options.services.postmodern = {
            enable = lib.mkEnableOption "postmodern job queue";

            database = {
              createLocally = lib.mkOption {
                type = lib.types.bool;
                default = true;
                description = "Whether to create the database locally.";
              };

              name = lib.mkOption {
                type = lib.types.str;
                default = "postmodern";
                description = "Name of the PostgreSQL database.";
              };

              allowedUsers = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
                description = "PostgreSQL roles granted access to the postmodern database.";
              };
            };
          };

          config = lib.mkIf cfg.enable {
            # TODO: implement service
          };
        };
    };
}
