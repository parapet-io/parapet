package io.parapet.raft

/** Pure state-transition function executed by a [[RaftNode]] once an entry is committed.
  *
  * Implementations MUST be deterministic and side-effect free — every replica applies
  * the same sequence of commands to the same initial state and is expected to produce
  * the same resulting state.
  *
  * @tparam State   immutable application state.
  * @tparam Command immutable command type contained in [[LogEntry]].
  */
trait RaftStateMachine[State, Command]:
  /** Applies `command` to `state` and returns the next state. */
  def apply(state: State, command: Command): State

object RaftStateMachine:
  /** Lift a function into a [[RaftStateMachine]]. */
  def apply[State, Command](f: (State, Command) => State): RaftStateMachine[State, Command] =
    new RaftStateMachine[State, Command]:
      def apply(state: State, command: Command): State =
        f(state, command)
