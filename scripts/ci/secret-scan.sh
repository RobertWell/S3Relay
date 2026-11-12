#!/bin/sh
# HEL-841: block a secret before it lands. Two passes, values redacted in every output:
#   1. the tree as committed (gitleaks dir .);
#   2. the FULL history of HEAD (gitleaks git .), not a push range: a range from the push's before-SHA misses
#      commits whose pipeline never ran (ops-loop pushes, a cancelled run, a force push), so every pipeline
#      scans everything (~15 s here). A shallow clone cannot do that, so it fails instead of passing.
# Known findings (11-11 sweep: fixtures, placeholders, old commits whose values are revoked or rotated) are in
# .gitleaksignore as exact fingerprints (file:rule:line, commit:file:rule:line): a NEW finding, even in one of
# those files, fails. A real credential is rotated, never added there.
set -eu
CFG=${GITLEAKS_CONFIG:-.gitleaks.toml}
IGN=${GITLEAKS_IGNORE:-.gitleaksignore}
OUT=${GITLEAKS_REPORT:-gitleaks-report.json}
# the job image runs as another uid than the checkout: let git read it
git config --global --add safe.directory "$PWD" 2>/dev/null || true
rc=0
if [ "$(git rev-parse --is-shallow-repository 2>/dev/null)" != "false" ]; then
  echo "SECRET SCAN: shallow or missing clone - the history cannot be scanned (fail closed); set GIT_DEPTH: 0"
  exit 1
fi
echo "=== secret scan: tree at $(git rev-parse --short HEAD)"
gitleaks dir . --config "$CFG" --gitleaks-ignore-path "$IGN" --redact=100 --no-banner --exit-code 1 -f json -r "$OUT" || rc=1
echo "=== secret scan: full history of HEAD ($(git rev-list --count HEAD) commits)"
# HEAD's history only: a clone that fetched every branch (hello-sre's .clone_internal) would otherwise scan
# other branches too (CI job 25683: 853 commits, 21 findings on old branches); a branch is scanned in its own pipeline
gitleaks git . --log-opts="HEAD" --config "$CFG" --gitleaks-ignore-path "$IGN" --redact=100 --no-banner --exit-code 1 -f json -r "commits-$OUT" || rc=1
[ "$rc" = 0 ] && echo "secret scan: clean" || echo "SECRET SCAN FAILED: rotate the credential, then remove it (values are redacted above)"
exit "$rc"
