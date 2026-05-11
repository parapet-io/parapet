# parapet-core

`parapet-core` is the effect-polymorphic Parapet framework.

It defines the process model, scheduler, DSL, interpreter, message abstractions, syntax, and the capability interfaces
required from an effect backend:

- `Effect[F]`
- `EffectFiber[F, A]`
- `Parallel[F]`
- `SchedulerRuntime[F]`
