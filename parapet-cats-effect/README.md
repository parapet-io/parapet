# parapet-cats-effect

`parapet-cats-effect` is the recommended production backend for running Parapet on Cats Effect `IO`.

It provides:

- `Effect[IO]`
- `Parallel[IO]`
- internal `SchedulerRuntime[IO]`
- `CatsEffectParApp` as a convenience application base

This module owns runtime integration details. `parapet-core` remains effect-polymorphic and does not depend on Cats
Effect.
