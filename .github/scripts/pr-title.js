// Validates the PR title shape (`type!?: subject`, <= 50 chars with the
// trailing ` (#N)` stripped). Dependabot PRs are exempt. Consumed by the
// Triage workflow via actions/github-script `require`.
module.exports = async ({ context, core }) => {
  const pr = context.payload.pull_request;
  if (pr.user.login === "dependabot[bot]") {
    core.notice("Dependabot PR - title gate exempt");
    return;
  }
  const types = "feat|fix|perf|refactor|docs|test|build|ci|chore|revert";
  const title = pr.title.replace(/ \(#\d+\)$/, "");
  const re = new RegExp(`^(${types})!?: [a-zA-Z].+[^.]$`);
  if (!re.test(title)) {
    core.setFailed(`Invalid PR title: "${title}". Expected type: subject.`);
    return;
  }
  if (title.length > 50) {
    core.setFailed(`PR title too long: ${title.length}/50 - "${title}".`);
  }
  if (/\b(?:add|added)\b/i.test(title)) {
    core.setFailed(
      `Weak verb in "${title}": "add"/"added" is banned in titles. Use a precise verb such as grant, introduce, establish, or wire up.`,
    );
  }
};
