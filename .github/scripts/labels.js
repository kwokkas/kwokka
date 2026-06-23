// All PR label automation in one script, consumed by the Labels workflow via
// actions/github-script. Area labels are derived from changed-file paths here,
// replacing the declarative labeler.yml plus the actions/labeler step.
//
//   1. Area `A-*` labels from changed paths (crate, ci, build, docs).
//   2. `unsafe` when a .rs diff adds a real unsafe construct (unsafe
//      fn/impl/trait/extern, or an unsafe block). Comment and string
//      mentions of the word do not count.
//   3. `kwokka` (a maintainer work unit) when the author is OWNER or MEMBER.
//   4. Require at least one `A-*` area label, failing the PR otherwise.

const areaFor = (path) => {
  const crate = path.match(/^crates\/kwokka-([a-z]+)\//);
  if (crate) return `A-${crate[1]}`;
  if (path.startsWith("crates/kwokka/")) return "A-facade";
  if (path.startsWith(".github/")) return "A-ci";
  if (path === ".coderabbit.yaml") return "A-ci";
  if (/^docs\//.test(path) || /\.md$/.test(path) || /^LICENSE/.test(path)) {
    return "A-docs";
  }
  if (
    /\.toml$/.test(path) ||
    path === "Cargo.lock" ||
    path.startsWith(".cargo/") ||
    path.startsWith(".config/") ||
    path.startsWith(".devcontainer/") ||
    path === ".gitattributes" ||
    path === ".gitignore"
  ) {
    return "A-build";
  }
  return null;
};

module.exports = async ({ github, context, core }) => {
  const { owner, repo } = context.repo;
  const pr = context.payload.pull_request;
  const issue_number = pr.number;

  const { data: files } = await github.rest.pulls.listFiles({
    owner,
    repo,
    pull_number: issue_number,
    per_page: 100,
  });

  const labels = new Set();
  for (const f of files) {
    const area = areaFor(f.filename);
    if (area) labels.add(area);
  }

  // Only an added line of real Rust code (not a comment) that uses an
  // unsafe construct counts. This keeps the label tied to actual unsafe,
  // not to a crate path or a prose mention of the word.
  const addsUnsafe = (f) =>
    f.filename.endsWith(".rs") &&
    (f.patch || "").split("\n").some((l) => {
      if (!l.startsWith("+") || l.startsWith("+++")) return false;
      const code = l.slice(1).trimStart();
      if (code.startsWith("//") || code.startsWith("*") || code.startsWith("/*")) {
        return false;
      }
      return (
        /\bunsafe\s+(?:fn|impl|trait|extern)\b/.test(code) ||
        /\bunsafe\s*\{/.test(code)
      );
    });
  if (files.some(addsUnsafe)) {
    labels.add("unsafe");
  }

  if (pr.author_association === "OWNER" || pr.author_association === "MEMBER") {
    labels.add("kwokka");
  }

  if (labels.size) {
    await github.rest.issues.addLabels({
      owner,
      repo,
      issue_number,
      labels: [...labels],
    });
  }

  if (![...labels].some((l) => l.startsWith("A-"))) {
    core.setFailed("PR needs at least one A-* area label (no mapped path changed)");
  }
};
