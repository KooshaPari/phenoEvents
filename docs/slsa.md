# SLSA Build Attestation

This repository publishes build provenance for release artifacts in
accordance with [SLSA (Supply-chain Levels for Software Artifacts)][slsa]
Build specifications. SLSA provenance allows downstream consumers to
verify that an artifact was built from the expected source repository,
at the expected commit, by the expected build platform.

## Target Level

**Current target: SLSA Build L2 (workflow repaired; hosted evidence pending)**

The release pipeline is hosted on GitHub Actions, an isolated build
platform that is owned and administered by GitHub. The repaired workflow
uses the official `actions/attest-build-provenance` action for published
releases. Provenance is signed through a GitHub-hosted OIDC flow and stored in
the [GitHub Artifact Attestations][ghaa] log alongside the artifact; a real
versioned-tag run is still required before this is marked achieved.

| Requirement                                 | Status       |
| ------------------------------------------- | ------------ |
| Provenance generated automatically          | ⏳ pending run |
| Provenance distributed alongside artifact   | ⏳ pending run |
| Build platform hosted and isolated          | ⏳ pending run |
| Provenance authenticity (OIDC-signed)       | ⏳ pending run |
| Build platform isolated from build request | ⏭ L3 target |
| Hardened build platform                     | ⏭ L3 target |
| Provenance non-forgeable (sigstore/cosign)  | ⏭ L3 target |

## Workflow

The CI workflow lives at
[`.github/workflows/release-attestation.yml`](../.github/workflows/release-attestation.yml)
and is triggered:

- Automatically on every `release: published` event.
- Manually via `workflow_dispatch`, but only when the selected ref is a
  versioned `vX.Y.Z` tag; branch attestations fail closed.

### Build Steps

1. **Checkout** — full history (`fetch-depth: 0`) so the git revision
   can be embedded in provenance.
2. **Toolchain** — pinned `stable` Rust via
   [`dtolnay/rust-toolchain`][rust-toolchain].
3. **Cache** — cargo registry, git index, and `target/` via
   [`Swatinem/rust-cache`][rust-cache].
4. **Build** — `cargo build --release --locked --workspace --all-targets`.
5. **Stage** — collect built executables, source tarball, and a build
   manifest into `release-artifacts/`.
6. **Upload** — publish the staged directory as the named
   `release-artifacts.zip` GitHub Actions artifact (90 day retention).
7. **Attest** — generate GitHub Artifact Attestation provenance with the
   pinned `actions/attest-build-provenance@v2` action over the uploaded archive
   digest returned by `actions/upload-artifact`.

## Verification

Consumers can verify a release artifact's provenance using the
[GitHub CLI][gh-cli]:

```bash
gh attestation verify <artifact> --owner <org>
```

The GitHub CLI verification is authoritative for this workflow. Do not infer
cosign certificate identity or L2 completion until a real versioned-tag run
records the attestation bundle and its verified subject digest.

The in-toto provenance attestation (`actions/attest-build-provenance`)
contains:

- `builder.id` — `https://github.com/actions/runner`
- `invocation.config.source.uri` — repository URL
- `invocation.config.source.entryPoint` — build workflow path
- `invocation.config.source.digest.sha1` — git commit SHA
- `invocation.config.source.ref` — git ref (tag / branch)
- `metadata.buildInvocationID` — workflow run ID
- `metadata.completeness.parameters` — whether all inputs are hashed
- `metadata.completeness.environment` — whether environment is fully captured

## Path to SLSA Build L3

The repaired pipeline targets L2; a real release or manual dispatch run and
artifact verification are still required before claiming L2. To graduate to L3, the following
additions are required:

1. **Isolated build environment** — move from a hosted runner to
   ephemeral, single-tenant builders (e.g.
   `slsa-framework/slsa-github-generator`'s `generator_container_slsa3.yml`
   reusable workflow, or a self-hosted runner with a hardened image).
2. **Provenance non-forgeability** — the generator workflow re-signs
   provenance with a build-platform-held signing key (sigstore / KMS)
   rather than relying on the GitHub OIDC token alone.
3. **Provenance transparency log** — the generator publishes
   provenance to a transparency log (e.g. Rekor) so forgery is
   detectable by the wider community.

To upgrade, switch the `attest-build-provenance` step to invoke the
`slsa-framework/slsa-github-generator/.github/workflows/generator_container_slsa3.yml@v2`
reusable workflow with a build image pinned by digest. The reusable
workflow handles ephemeral runners, hardened isolation, and
non-forgeable provenance signing transparently.

## References

- [SLSA Framework][slsa]
- [`slsa-framework/slsa-github-generator`][slsa-gh-gen]
- [GitHub Artifact Attestations][ghaa]
- [GitHub Actions security hardening][ghas]
- [`dtolnay/rust-toolchain`][rust-toolchain]
- [`Swatinem/rust-cache`][rust-cache]
- [`cosign`][cosign]

[slsa]: https://slsa.dev
[slsa-gh-gen]: https://github.com/slsa-framework/slsa-github-generator
[ghaa]: https://docs.github.com/en/security/supply-chain-security/artifact-attestations
[ghas]: https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions
[gh-cli]: https://cli.github.com
[cosign]: https://github.com/sigstore/cosign
[rust-toolchain]: https://github.com/dtolnay/rust-toolchain
[rust-cache]: https://github.com/Swatinem/rust-cache
