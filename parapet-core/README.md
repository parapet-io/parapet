# parapet-core

`parapet-core` is the effect-polymorphic Parapet framework.

It defines the process model, scheduler, DSL, interpreter, message abstractions, syntax, and the capability interfaces
required from an effect backend:

- `Effect[F]`
- `EffectFiber[F, A]`
- `Parallel[F]`
- `SchedulerRuntime[F]`

It intentionally does not provide or own a production effect runtime. Use `parapet-cats-effect`, `parapet-pario`, or a
custom backend to run applications.
