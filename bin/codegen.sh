#!/usr/bin/env bash
set -euo pipefail

# Pinned nixpkgs revision so the pg_dump used for code generation is
# reproducible instead of tracking a moving registry ref. nixos-25.05
# provides postgresql_17 (narHash sha256-16KkgfdYqjaeRGBaYsNrhPRRENs0qzkQVUooNHtoy2w=).
NIXPKGS_REF="github:NixOS/nixpkgs/ac62194c3917d5f474c1a844b6fd6da2db95077d"
SQL="src/gabsurd/sql.gleam"

# Parrot 2.2.1 always emits this import, even when no generated binding uses
# Option. Keep the normalization here so $SQL remains a reproducible output of
# the code-generation workflow rather than a hand edit.
#
# `nix build` (not `nix eval`) realizes/substitutes the store path so a clean
# machine has pg_dump available; `--print-out-paths` prints it for PATH use.
# `^out` selects the single binary output (out, not man) to match the original.
PATH="$(nix build --no-link --print-out-paths "${NIXPKGS_REF}#postgresql_17^out")/bin:$PATH" gleam run -m parrot

if ! awk 'NR > 1 && $0 !~ /import gleam\/option\.\{type Option\}/ { if (index($0, "Option")) found = 1 } END { exit !found }' "$SQL"; then
  # Edit through a `.bak` and discard it: BSD/macOS sed require an argument to
  # `-i`, so a backup suffix stays portable where the GNU-only bare `sed -i` does not.
  sed -i.bak '/^import gleam\/option\.{type Option}$/d' "$SQL"
  rm -f "${SQL}.bak"
fi
