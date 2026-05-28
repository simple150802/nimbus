package online

import "nimbus/api/nimbusevent"

// buildOnlineStatus wraps the per-ksvc assignment rows in the OnlineStatus
// payload written to .status.online. ActiveAssignments is kept in sync with
// len(Assignments) so a `kubectl get -o wide` style read needs only the
// scalar. BurstMode records the cluster-wide detector state that drove this
// reconcile, for experiment-CSV correlation. Old rows are replaced wholesale
// each tick (merge-patch on the status subresource replaces the array), so
// this always reflects the most recent reconcile.
func buildOnlineStatus(assignments []nimbusevent.OnlineAssignment, mode BurstMode) *nimbusevent.OnlineStatus {
	return &nimbusevent.OnlineStatus{
		ActiveAssignments: len(assignments),
		BurstMode:         mode.String(),
		Assignments:       assignments,
	}
}
