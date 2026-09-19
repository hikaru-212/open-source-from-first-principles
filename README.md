# Open Source from First Principles

[English](README.md) | [繁體中文](README.zh-TW.md)

Most onboarding material begins with APIs and terminology. This repository starts with project positioning, then reasons from physical constraints, failure modes, invariants, and design trade-offs to source code, tests, issues, and real contribution workflows.

The goal is to help technically capable newcomers become **Contribution Ready**, not merely learn an API.

## Tracks

| Track | zh-TW | English | Status |
| --- | --- | --- | --- |
| Kafka | [閱讀](docs/kafka/zh-TW/README.md) | [Read](docs/kafka/en/README.md) | Prototype for Review |
| Ray | [閱讀](docs/ray/zh-TW/README.md) | [Read](docs/ray/en/README.md) | Prototype for Review |
| YuniKorn | Planned | Planned | Planned |

Future infrastructure tracks can follow the same language and content layout under `docs/`.

## Teaching Method

```text
Project Positioning
→ Opening Question
→ Physical Problem
→ Physical Constraint
→ Design Choice
→ Mechanism
→ Guarantee
→ Non-guarantee
→ Failure Boundary
→ Test / Source / Issue
→ Closing Synthesis
```

Each track keeps the main learning path short while connecting bounded guarantees to the upstream evidence a contributor needs.

**Layer 0 — Project Positioning is mandatory for every current and future track.** Before asking “why is the system designed this way?”, establish its system category, practical problem, owned decision, and non-responsibility boundary in a short introduction. Before first-principles analysis, every track must pass this check: “Can a newcomer explain what this project does in a few sentences before we ask why its mechanisms exist?”

**Every track must also connect an Opening Question to a Closing Synthesis.** After Layer 0, ask: “If we did not use this project, what engineering burden would exist?” After the main path, answer: “Now that we understand the mechanisms, what responsibility does this project actually take off our plate?” Identify work that custom code or another system would need to handle; then connect the reviewed mechanisms to the responsibility they take on and the work that remains with users/operators. This framing is mandatory for future tracks and does not replace technical depth or failure boundaries.

## Methodology Provenance

The teaching approach in this repository grew out of the engineering reasoning documented in **Streaming System + Compass**, created by **Yen-Hua Chen**.

The methodology was first concretely developed through the Kafka track. The Ray track then applied the same process:

1. build a Compass-derived reasoning profile;
2. independently verify each project's actual mechanisms;
3. reject analogies whose physical mechanisms do not match;
4. map physical problems into bounded guarantees and non-guarantees;
5. connect the result to project-specific documentation, source, tests, and issues.

This repository tests whether one first-principles onboarding methodology can be reused across different infrastructure projects while independently verifying each project's actual mechanisms. It does not claim that Kafka or Ray architecture originated from Compass.

Original methodology source: [https://github.com/hikaru-212/streaming-system-compass](https://github.com/hikaru-212/streaming-system-compass)

## License

Documentation and educational prose are licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Software and substantial standalone executable examples are licensed under the [Apache License 2.0](LICENSE). Short illustrative snippets embedded in documentation remain part of the CC BY 4.0 document.

See [LICENSE-CONTENT.md](LICENSE-CONTENT.md) for the complete licensing map.

## Languages

The English landing page is this file. See the [Traditional Chinese landing page](README.zh-TW.md) for `zh-TW`.
