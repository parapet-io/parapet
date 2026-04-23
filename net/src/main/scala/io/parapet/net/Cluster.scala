package io.parapet.net

import io.parapet.ProcessRef

final case class GroupId(value: String) extends AnyVal
final case class MemberId(value: String) extends AnyVal

final case class ClusterMember(
    id: MemberId,
    process: ProcessRef,
    addresses: Map[TransportProtocol, NetworkAddress],
    groups: Set[GroupId] = Set.empty
)

final case class ClusterView(members: Map[MemberId, ClusterMember]):
  def groups: Set[GroupId] =
    members.values.flatMap(_.groups).toSet

  def membersOf(groupId: GroupId): Vector[ClusterMember] =
    members.valuesIterator.filter(_.groups.contains(groupId)).toVector

  def member(id: MemberId): Option[ClusterMember] =
    members.get(id)
