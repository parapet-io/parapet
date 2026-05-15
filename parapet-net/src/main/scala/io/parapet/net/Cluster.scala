package io.parapet.net

import io.parapet.ProcessRef

/** Logical group identifier; lets members opt into named subsets such as `"raft-leaders"` or `"frontends"`.
  */
final case class GroupId(value: String) extends AnyVal

/** Stable identifier for a single node in the cluster. */
final case class MemberId(value: String) extends AnyVal

/** Metadata about one cluster member.
  *
  * @param id
  *   unique node identity.
  * @param process
  *   local [[ProcessRef]] proxy used to talk to this member.
  * @param addresses
  *   set of addresses this member is reachable on (one per transport).
  * @param groups
  *   logical groups the member belongs to.
  */
final case class ClusterMember(
    id: MemberId,
    process: ProcessRef[io.parapet.Event],
    addresses: Map[TransportProtocol, NetworkAddress],
    groups: Set[GroupId] = Set.empty
)

/** Snapshot of cluster membership.
  *
  * Read-mostly view derived from gossip - see [[io.parapet.core.api.ClusterState]] for the wire-level form. Provides
  * convenience lookups by id and by group.
  */
final case class ClusterView(members: Map[MemberId, ClusterMember]):
  /** All distinct groups represented in this view. */
  def groups: Set[GroupId] =
    members.values.flatMap(_.groups).toSet

  /** Members belonging to `groupId`. */
  def membersOf(groupId: GroupId): Vector[ClusterMember] =
    members.valuesIterator.filter(_.groups.contains(groupId)).toVector

  /** Member with the given id, if known. */
  def member(id: MemberId): Option[ClusterMember] =
    members.get(id)
