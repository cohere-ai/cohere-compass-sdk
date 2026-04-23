# OSS Contributing Guide

Thank you for your interest in contributing to Cohere's Compass SDK. This guide will help explain the contribution workflow from opening an issue, creating a PR, to reviewing and merging your added changes.

## How to Contribute

1. Ensure your change has an issue! Find an existing issue or open a new issue.
   - This is where you can get a feel if the change will be accepted or not.
2. Once approved, fork this repository in your own GitHub account.
3. [Fork this repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo)
4. Make your changes on your fork and make sure all your [commits are signed](https://docs.github.com/en/authentication/managing-commit-signature-verification/about-commit-signature-verification)!
5. [Submit the fork as a Pull Request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) pointing to the `main` branch this repository. A maintainer should comment and/or review your Pull Request within a few days. Although depending on the circumstances, it may take longer.

## Version Bump Policy

Any PR that changes the SDK source code (`cohere_compass/`) or the package config (`pyproject.toml`) **must** include a version bump in the `version` field of `pyproject.toml`. A CI check will block the PR if the version has not been updated.

PRs that only touch documentation, tests, CI configs, or examples are exempt — the check will not run for those changes.

If your PR is an exception (e.g., a dependency-only change in `pyproject.toml` that doesn't warrant a release), a maintainer can add the **`skip-version-check`** label to bypass the check.