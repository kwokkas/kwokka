#!/usr/bin/env bash
set -euo pipefail

# Pin actionlint to a known release, then lint every workflow.
version=1.7.12
curl -sSL "https://github.com/rhysd/actionlint/releases/download/v${version}/actionlint_${version}_linux_amd64.tar.gz" \
  | tar xz -C /usr/local/bin actionlint
actionlint
