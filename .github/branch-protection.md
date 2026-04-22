# Branch Protection Configuration for `main`

This document describes the exact branch protection settings that must be applied to the
`main` branch in the GitHub UI. These settings cannot be applied via the `gh` CLI in this
project's workflow; the maintainer must configure them manually.

**Why these settings matter**: The `main` branch is the single source of truth for all
released versions. Preventing direct pushes and requiring CI to pass before merge ensures
that no broken or unreviewed code can reach a published release.

---

## Step-by-Step Configuration

Navigate to: **GitHub → Repository Settings → Branches → Add branch ruleset** (or
"Add branch protection rule" on the classic UI).

Set the branch name pattern to: `main`

### 1. Require a pull request before merging

- Check: **Require a pull request before merging**
- Set **Required number of approvals** to: `1`
- Leave "Dismiss stale pull request approvals when new commits are pushed" checked
  (recommended — forces re-review after force-pushed changes)
- Do not check "Require review from code owners" unless a `CODEOWNERS` file is in place

### 2. Require status checks to pass before merging

- Check: **Require status checks to pass before merging**
- Check: **Require branches to be up to date before merging**
  (this prevents a branch that was green against an old `main` from merging after
  a conflicting change lands)

Add each of the following required status checks by entering the exact job name into the
search field. These names come directly from the workflow YAML files:

| Workflow file                          | Exact job name to enter    |
|----------------------------------------|----------------------------|
| `.github/workflows/ci.yml`             | `build`                    |
| `.github/workflows/dependency-check.yml` | `owasp-dependency-check` |

> Note: GitHub only surfaces a job name in the status-check search box after that job has
> run at least once against the repository. If the jobs do not appear, trigger the
> workflows once (e.g. by pushing a trivial commit to a feature branch that opens a PR)
> and then return here to add them.

### 3. Do not allow bypassing the above settings

- Check: **Do not allow bypassing the above settings**

This ensures that even repository admins (including the maintainer) cannot merge directly
to `main` without a passing CI run and at least one approving review. This is the correct
posture for a compliance-oriented open-source project — the rules apply to everyone.

### 4. Restrict who can push to matching branches (optional)

Restricting direct-push access to `main` is enforced automatically by the PR requirement
above (no one can push directly; all changes must go through a PR). The additional
"Restrict pushes that create matching branches" option is left to maintainer judgement.
For a single-maintainer repository it is not necessary.

---

## Summary Checklist

Use this checklist when applying the settings:

- [ ] Branch name pattern set to `main`
- [ ] "Require a pull request before merging" enabled, minimum 1 approving review
- [ ] "Require status checks to pass before merging" enabled
- [ ] Required status check added: `build`
- [ ] Required status check added: `owasp-dependency-check`
- [ ] "Require branches to be up to date before merging" enabled
- [ ] "Do not allow bypassing the above settings" enabled

---

## Related Workflow Files

- `.github/workflows/ci.yml` — Compile, test, and package the Spark application
- `.github/workflows/dependency-check.yml` — OWASP dependency vulnerability scan

These files must exist and have produced at least one run before GitHub will resolve the
status check names. If they were merged as part of the same issue that introduced this
document, trigger a workflow run on a test branch before returning to apply these settings.
