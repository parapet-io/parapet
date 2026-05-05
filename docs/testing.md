# Parapet testing

Run the scheduler stress tests:

```bash
sbt schedulerStress                                                                # infinite, until failure
sbt -Dscheduler.stress.iterations=200 schedulerStress                              # bounded, 200 iterations
sbt -Dscheduler.stress.seed=12345 -Dscheduler.stress.iterations=50 schedulerStress # reproduce
```
