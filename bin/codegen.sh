#!/usr/bin/env bash
set -euo pipefail

# Parrot 2.2.1 always emits this import, even when no generated binding uses
# Option. Keep the normalization here so src/gabsurd/sql.gleam remains a
# reproducible output of the code-generation workflow rather than a hand edit.
PATH="$(nix eval --raw 'nixpkgs#postgresql_17.out')/bin:$PATH" gleam run -m parrot

if ! awk 'NR > 1 && $0 !~ /import gleam\/option\.\{type Option\}/ { if (index($0, "Option")) found = 1 } END { exit !found }' src/gabsurd/sql.gleam; then
  sed -i '/^import gleam\/option\.{type Option}$/d' src/gabsurd/sql.gleam
fi
