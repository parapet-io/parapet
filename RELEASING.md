# Releasing Parapet

Releases are derived from Git tags. A tag matching `vMAJOR.MINOR.PATCH` or
`vMAJOR.MINOR.PATCH-PRERELEASE` starts the release workflow, which:

1. verifies that the tagged commit belongs to `main`;
2. checks formatting and runs the test suites;
3. signs and publishes the artifacts to Maven Central; and
4. creates a GitHub release with generated release notes.

The GitHub release is created only after Maven Central accepts the artifacts.

## Create a release

Start from an up-to-date `main` branch, then create and push an annotated tag:

```bash
git switch main
git pull --ff-only
git tag -a v0.1.0 -m "v0.1.0"
git push origin v0.1.0
```

For a prerelease, use a tag such as `v0.1.0-RC1`. The workflow marks tags containing a prerelease
suffix as GitHub prereleases.

Published Maven Central versions are immutable. If publication succeeds but GitHub release
creation fails, rerun only the failed `Create GitHub release` job rather than the successful Maven
publication job.
