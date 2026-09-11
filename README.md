# Open Source from First Principles

[English](README.md) | [繁體中文](README.zh-TW.md)

Most onboarding material begins with APIs and terminology. This repository instead starts from physical constraints, failure modes, invariants, and design trade-offs, then connects those ideas to source code, tests, issues, and real contribution workflows.

The goal is to help technically capable newcomers become **Contribution Ready**, not merely learn an API.

## Tracks

| Track | zh-TW | English | Status |
| --- | --- | --- | --- |
| Kafka | [閱讀](docs/kafka/zh-TW/README.md) | [Read](docs/kafka/en/README.md) | Prototype for Review |
| Ray | [規劃中](docs/ray/zh-TW/README.md) | [Planned](docs/ray/en/README.md) | Planned |

Future infrastructure tracks can follow the same language and content layout under `docs/`.

## Teaching Method

```text
Problem
→ Physical Constraint
→ Design Choice
→ Mechanism
→ Guarantee
→ Non-guarantee
→ Failure Boundary
→ Test / Source / Issue
```

Each track keeps the main learning path short while connecting bounded guarantees to the upstream evidence a contributor needs.

## Methodology Provenance

The teaching approach in this repository grew out of the engineering reasoning documented in **Streaming System + Compass**, created by **Yen-Hua Chen**.

Rather than asking an AI system to generate a conventional tutorial, the initial Kafka track was developed by:

1. extracting recurring reasoning patterns from Compass ADRs, postmortems, reasoning notes, and design philosophy;
2. independently checking those patterns against Apache Kafka documentation, Javadocs, KIPs, source code, and tests;
3. rejecting analogies whose physical mechanisms did not actually match Kafka;
4. turning the surviving reasoning patterns into a first-principles onboarding path.

The goal is not to claim that Kafka's design originated from Compass, but to make the origin of this teaching methodology and framing explicit.

Original methodology source: [https://github.com/hikaru-212/streaming-system-compass](https://github.com/hikaru-212/streaming-system-compass)

## License

Documentation and educational prose are licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Software and substantial standalone executable examples are licensed under the [Apache License 2.0](LICENSE). Short illustrative snippets embedded in documentation remain part of the CC BY 4.0 document.

See [LICENSE-CONTENT.md](LICENSE-CONTENT.md) for the complete licensing map.

## Languages

The English landing page is this file. See the [Traditional Chinese landing page](README.zh-TW.md) for `zh-TW`.
