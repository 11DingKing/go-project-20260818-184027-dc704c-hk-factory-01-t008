package change

import "regdispatch/internal/errorsx"

// Action is the verb that drives a state transition.
type Action string

const (
	ActionSubmit               Action = "submit"
	ActionDispatch             Action = "dispatch"
	ActionAckPartial           Action = "ack_partial"
	ActionAckAll               Action = "ack_all"
	ActionFailPartial          Action = "fail_partial"
	ActionFailAll              Action = "fail_all"
	ActionStartCompensation    Action = "start_compensation"
	ActionCompensationDone     Action = "compensation_done"
	ActionCompensationResolved Action = "compensation_resolved"
	ActionRevoke               Action = "revoke"
)

// transitionRule defines a legal (from, action) → to mapping.
type transitionRule struct {
	from   Status
	action Action
	to     Status
}

// transitionTable is the explicit, immutable state machine for a Change.
// Any (from, action) pair not in this table is rejected as illegal.
var transitionTable = []transitionRule{
	{StatusDraft, ActionSubmit, StatusSubmitted},
	{StatusSubmitted, ActionDispatch, StatusDispatching},
	{StatusDispatching, ActionAckPartial, StatusPartialSuccess},
	{StatusDispatching, ActionAckAll, StatusCompleted},
	{StatusDispatching, ActionFailPartial, StatusPartialFailed},
	{StatusDispatching, ActionFailAll, StatusPartialFailed},
	{StatusPartialSuccess, ActionFailPartial, StatusPartialFailed},
	{StatusPartialFailed, ActionStartCompensation, StatusCompensating},
	{StatusPartialSuccess, ActionStartCompensation, StatusCompensating},
	{StatusCompensating, ActionCompensationDone, StatusRolledBack},
	{StatusCompensating, ActionCompensationResolved, StatusCompleted},
	// Revocation is allowed from any non-terminal, non-revoked state.
	{StatusDraft, ActionRevoke, StatusRevoked},
	{StatusSubmitted, ActionRevoke, StatusRevoked},
	{StatusDispatching, ActionRevoke, StatusRevoked},
	{StatusPartialSuccess, ActionRevoke, StatusRevoked},
	{StatusPartialFailed, ActionRevoke, StatusRevoked},
	{StatusCompensating, ActionRevoke, StatusRevoked},
}

// IsTerminal returns true if no further transitions are possible.
func IsTerminal(s Status) bool {
	return s == StatusCompleted || s == StatusRevoked || s == StatusRolledBack
}

// CanTransition checks whether the transition is legal without performing it.
func CanTransition(from Status, action Action) bool {
	for _, r := range transitionTable {
		if r.from == from && r.action == action {
			return true
		}
	}
	return false
}

// Transition applies an action to the current status and returns the new
// status, or an InvalidTransition error if the pair is not in the table.
func Transition(from Status, action Action) (Status, error) {
	for _, r := range transitionTable {
		if r.from == from && r.action == action {
			return r.to, nil
		}
	}
	return from, errorsx.InvalidTransition(string(from), string(action), "change")
}

// LegalActions returns all actions valid for the given status.
func LegalActions(s Status) []Action {
	var actions []Action
	for _, r := range transitionTable {
		if r.from == s {
			actions = append(actions, r.action)
		}
	}
	return actions
}

// DispatchTransitionTable defines the dispatch task state machine.
type dispatchTransitionRule struct {
	from   DispatchStatus
	action string
	to     DispatchStatus
}

var dispatchTransitionTable = []dispatchTransitionRule{
	{DispatchPending, "deliver", DispatchDelivered},
	{DispatchDelivered, "ack", DispatchAcked},
	{DispatchAcked, "start", DispatchProcessing},
	{DispatchAcked, "succeed", DispatchSucceeded},
	{DispatchAcked, "fail", DispatchFailed},
	{DispatchProcessing, "succeed", DispatchSucceeded},
	{DispatchProcessing, "fail", DispatchFailed},
	{DispatchDelivered, "timeout", DispatchTimedOut},
	{DispatchAcked, "timeout", DispatchTimedOut},
	{DispatchTimedOut, "redeliver", DispatchPending},
	{DispatchFailed, "dead_letter", DispatchDeadLetter},
	{DispatchFailed, "retry", DispatchPending},
	{DispatchTimedOut, "escalate", DispatchPending},
}

// CanDispatchTransition checks legality of a dispatch transition.
func CanDispatchTransition(from DispatchStatus, action string) bool {
	for _, r := range dispatchTransitionTable {
		if r.from == from && r.action == action {
			return true
		}
	}
	return false
}

// DispatchTransition applies an action to a dispatch status.
func DispatchTransition(from DispatchStatus, action string) (DispatchStatus, error) {
	for _, r := range dispatchTransitionTable {
		if r.from == from && r.action == action {
			return r.to, nil
		}
	}
	return from, errorsx.InvalidTransition(string(from), action, "dispatch_task")
}

// DispatchIsTerminal reports whether the dispatch status is final.
func DispatchIsTerminal(s DispatchStatus) bool {
	return s == DispatchSucceeded || s == DispatchDeadLetter
}
