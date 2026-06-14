#!/usr/bin/env bash
set -euo pipefail

# zizmor reads its rule config from a file only, so synthesize the config
# here instead of carrying a separate .github/zizmor.yml. unpinned-uses is
# off (tag pins are intentional); triage runs on pull_request_target with a
# write-scoped label job, which is by design.
config="$(mktemp)"
trap 'rm -f "$config"' EXIT
cat > "$config" <<'YAML'
rules:
  unpinned-uses:
    disable: true
  dangerous-triggers:
    ignore:
      - triage.yml
  excessive-permissions:
    ignore:
      - triage.yml
YAML

zizmor --config "$config" --min-severity low .github/workflows/
