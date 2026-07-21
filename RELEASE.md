# Releasing minisqlite

## One-time setup

1. Create or log in to your [crates.io](https://crates.io) account.
2. Go to **Account Settings → API Tokens** and generate a new token.
3. Copy the token and add it to this GitHub repository as a secret named `CARGO_REGISTRY_TOKEN`:
   - GitHub repo → Settings → Secrets and variables → Actions → New repository secret
   - Name: `CARGO_REGISTRY_TOKEN`
   - Value: the token from crates.io

## Publishing a new version

1. Update `Cargo.toml` `version` and `CHANGELOG.md`.
2. Commit and push the changes.
3. Create and push a tag:

   ```bash
   git tag v0.3.0-alpha.1
   git push origin v0.3.0-alpha.1
   ```

4. The `release.yml` workflow will:
   - run `cargo test`
   - publish to crates.io
   - build a release binary
   - create a GitHub Release with the binary attached
