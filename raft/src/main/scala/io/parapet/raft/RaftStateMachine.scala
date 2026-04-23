package io.parapet.raft

trait RaftStateMachine[State, Command]:
  def apply(state: State, command: Command): State

object RaftStateMachine:
  def apply[State, Command](f: (State, Command) => State): RaftStateMachine[State, Command] =
    new RaftStateMachine[State, Command]:
      def apply(state: State, command: Command): State =
        f(state, command)
