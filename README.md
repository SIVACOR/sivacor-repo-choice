# sivacor-repo-choice

## Triggering a documentation rebuild

The `update-allowed-repos` workflow ([.github/workflows/update-allowed-repos.yml](.github/workflows/update-allowed-repos.yml))
regenerates `allowed_repos.yaml` and `allowed_repos_by_software.yaml` and commits
any changes. When either of those files actually changes, the workflow also
triggers a rebuild of the docs site by dispatching the `deploy.yml` workflow
in [SIVACOR/documentation](https://github.com/SIVACOR/documentation).

Cross-repo `workflow_dispatch` calls can't use the default `GITHUB_TOKEN` —
that token only has access to the repo the workflow runs in. A personal
access token (PAT) with access to `SIVACOR/documentation` must be stored here
as a repository secret named `DOCS_DISPATCH_TOKEN`.

### One-time setup

1. **Create a fine-grained PAT** at
   [github.com/settings/personal-access-tokens/new](https://github.com/settings/personal-access-tokens/new):
   - Resource owner: `SIVACOR`
   - Repository access: **Only select repositories** → `SIVACOR/documentation`
   - Permissions: **Actions** → **Read and write** (this is the only
     permission needed to run `gh workflow run`)
   - Set an expiration and note a reminder to rotate it before it lapses.

2. **Add the token as a secret on this repo**, either via the GitHub UI
   (Settings → Secrets and variables → Actions → New repository secret) or
   from the command line with the [GitHub CLI](https://cli.github.com/):

   ```bash
   gh secret set DOCS_DISPATCH_TOKEN --repo SIVACOR/sivacor-repo-choice
   ```

   This prompts for the token value on stdin (paste it and press
   Ctrl+D / Enter). To set it non-interactively from a file or variable
   instead:

   ```bash
   gh secret set DOCS_DISPATCH_TOKEN --repo SIVACOR/sivacor-repo-choice --body "$TOKEN_VALUE"
   ```

3. **Verify** the secret is present (this only confirms it exists, not its
   value):

   ```bash
   gh secret list --repo SIVACOR/sivacor-repo-choice
   ```

Once the secret is set, the next run of `update-allowed-repos` that changes
`allowed_repos.yaml` or `allowed_repos_by_software.yaml` will automatically
kick off a documentation rebuild.
