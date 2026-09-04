# Publish a release

Publishing a GitHub release is the irreversible gate that starts the NuGet workflow. Prepare and
validate the version on `main` before publishing the release.

## Configure trusted publishing once

1. Create a GitHub environment named `nuget`. Add reviewer protection if releases require approval.
2. In the nuget.org account that owns `ValkeyDotNet`, create a trusted publishing policy for:
   - owner: `kidoz`
   - repository: `valkey-dotnet`
   - workflow: `release.yml`
   - environment: `nuget`
3. Add `NUGET_USER` to the GitHub `nuget` environment as a secret. Its value is the nuget.org account
   username, not an email address or API key.

The workflow exchanges GitHub's OIDC token for a short-lived NuGet API key. Do not create or store a
long-lived `NUGET_API_KEY` for this workflow.

## Prepare a version

1. Set `<Version>` in `src/ValkeyDotNet/ValkeyDotNet.csproj`.
2. Add a `## [VERSION]` section to `CHANGELOG.md`.
3. Update the package version shown in `README.md`.
4. Run the complete local release gate:

   ```bash
   just ci
   just test-matrix
   just test-cluster
   just pack
   just cluster-down
   just valkey-down
   ```

5. Commit and push the prepared release to `main`.

## Exercise the workflow without publishing

Run the `Release` workflow manually from GitHub Actions. Manual dispatch runs the live integration
gate, builds `0.0.0-manual`, attests it, and uploads it as a workflow artifact. It never signs in to
nuget.org and never publishes a package.

## Publish

Create and publish a GitHub release with a strict SemVer tag such as `v1.0.0`. Use
`ValkeyDotNet 1.0.0` as the release title and copy the matching changelog section into its notes.

The release workflow:

1. validates the tag and matching changelog section;
2. runs the Valkey 9.1, 8.1, and 7.2 compatibility matrix and the three-primary cluster suite;
3. runs formatting, release build, and server-free tests;
4. packs and attests the `.nupkg`;
5. uploads the package as a GitHub Actions artifact;
6. obtains a short-lived credential through NuGet trusted publishing and pushes to nuget.org.

The NuGet push intentionally does not use `--skip-duplicate`. Attempting to publish an immutable
version twice fails loudly instead of reporting a false successful release.
