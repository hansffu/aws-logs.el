{
  description = "Emacs JSON log viewer with Rust worker";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
      forAllSystems = f:
        nixpkgs.lib.genAttrs systems
          (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAllSystems (pkgs:
        let
          inherit (pkgs) lib;
          hasCargoLock = builtins.pathExists ./Cargo.lock;
          worker = pkgs.rustPlatform.buildRustPackage {
            pname = "json-log-viewer-worker";
            version = "0.1.0";

            src = lib.cleanSourceWith {
              src = ./.;
              filter = path: type:
                let
                  rel = lib.removePrefix (toString ./. + "/") (toString path);
                in
                rel == "Cargo.toml"
                || rel == "Cargo.lock"
                || rel == "rust"
                || lib.hasPrefix "rust/" rel;
            };

            cargoLock.lockFile = ./Cargo.lock;
            doCheck = false;
          };
          lockfileRequired = pkgs.runCommand "json-log-viewer-worker-lockfile-required" { } ''
            echo "Cargo.lock is required for nix build." >&2
            echo "Run: nix develop -c cargo generate-lockfile" >&2
            echo "Then run: nix build .#json-log-viewer-worker" >&2
            exit 1
          '';
        in
        {
          json-log-viewer-worker = if hasCargoLock then worker else lockfileRequired;
          default = self.packages.${pkgs.system}.json-log-viewer-worker;
        });

      apps = forAllSystems (pkgs: {
        json-log-viewer-worker = {
          type = "app";
          program = "${self.packages.${pkgs.system}.json-log-viewer-worker}/bin/json-log-viewer-worker";
        };
        json-log-viewer-ingest-wrapper = {
          type = "app";
          program = "${self.packages.${pkgs.system}.json-log-viewer-worker}/bin/json-log-viewer-ingest-wrapper";
        };
        default = self.apps.${pkgs.system}.json-log-viewer-worker;
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            rustc
            rustfmt
            clippy
          ];
        };
      });
    };
}
