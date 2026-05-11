# parapet-testkit

`parapet-testkit` contains backend-agnostic conformance specs and test utilities for Parapet.

Backend modules depend on this in test scope and provide concrete runners for their effect type. For example,
`parapet-pario` runs these specs with `ParIO`, while `parapet-cats-effect` runs them with Cats Effect `IO`.

The testkit should depend only on `parapet-core`.
