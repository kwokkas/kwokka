#!/usr/bin/env bash
set -euo pipefail

# Pin actionlint to a known release, then lint every workflow. While the
# repo is private, every workflow runs on the self-hosted kwokka-runner, so
# register that label here instead of carrying a separate actionlint.yaml.
version=1.7.12
curl -sSL "https://github.com/rhysd/actionlint/releases/download/v${version}/actionlint_${version}_linux_amd64.tar.gz" \
  | tar xz -C /usr/local/bin actionlint

config="$(mktemp)"
trap 'rm -f "$config"' EXIT
cat > "$config" <<'YAML'
self-hosted-runner:
  labels:
    - kwokka-runner
YAML

actionlint -config-file "$config"
