package online

import "nimbus/api/nimbusevent"

// buildOnlineStatus wraps the per-ksvc assignment rows in the OnlineStatus
// payload written to .status.online. ActiveAssignments is kept in sync with
// len(Assignments) so a `kubectl get -o wide` style read needs only the
// scalar. Old rows are replaced wholesale each tick (merge-patch on the
// status subresource replaces the array), so this always reflects the most
// recent reconcile.
func buildOnlineStatus(assignments []nimbusevent.OnlineAssignment) *nimbusevent.OnlineStatus {
	return &nimbusevent.OnlineStatus{
		ActiveAssignments: len(assignments),
		Assignments:       assignments,
	}
}
