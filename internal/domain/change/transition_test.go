package change

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidTransitionRejected(t *testing.T) {
	tests := []struct {
		name   string
		from   Status
		action Action
	}{
		{"draft_to_ack_all", StatusDraft, ActionAckAll},
		{"completed_to_dispatch", StatusCompleted, ActionDispatch},
		{"revoked_to_submit", StatusRevoked, ActionSubmit},
		{"rolled_back_to_dispatch", StatusRolledBack, ActionDispatch},
		{"draft_to_fail_partial", StatusDraft, ActionFailPartial},
		{"completed_to_revoke", StatusCompleted, ActionRevoke},
		{"rolled_back_to_revoke", StatusRolledBack, ActionRevoke},
		{"submitted_to_ack_all", StatusSubmitted, ActionAckAll},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Transition(tt.from, tt.action)
			assert.Error(t, err, "transition from %s via %s should be rejected", tt.from, tt.action)
		})
	}
}

func TestLegalTransitions(t *testing.T) {
	tests := []struct {
		name     string
		from     Status
		action   Action
		expected Status
	}{
		{"draft_to_submitted", StatusDraft, ActionSubmit, StatusSubmitted},
		{"submitted_to_dispatching", StatusSubmitted, ActionDispatch, StatusDispatching},
		{"dispatching_to_completed", StatusDispatching, ActionAckAll, StatusCompleted},
		{"dispatching_to_partial_success", StatusDispatching, ActionAckPartial, StatusPartialSuccess},
		{"dispatching_to_partial_failed", StatusDispatching, ActionFailPartial, StatusPartialFailed},
		{"partial_failed_to_compensating", StatusPartialFailed, ActionStartCompensation, StatusCompensating},
		{"compensating_to_rolled_back", StatusCompensating, ActionCompensationDone, StatusRolledBack},
		{"compensating_to_completed", StatusCompensating, ActionCompensationResolved, StatusCompleted},
		{"dispatching_to_revoked", StatusDispatching, ActionRevoke, StatusRevoked},
		{"submitted_to_revoked", StatusSubmitted, ActionRevoke, StatusRevoked},
		{"draft_to_revoked", StatusDraft, ActionRevoke, StatusRevoked},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Transition(tt.from, tt.action)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestDispatchTransitionRejected(t *testing.T) {
	tests := []struct {
		name   string
		from   DispatchStatus
		action string
	}{
		{"pending_to_succeed", DispatchPending, "succeed"},
		{"delivered_to_fail", DispatchDelivered, "fail"},
		{"succeeded_to_ack", DispatchSucceeded, "ack"},
		{"dead_letter_to_retry", DispatchDeadLetter, "retry"},
		{"succeeded_to_deliver", DispatchSucceeded, "deliver"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DispatchTransition(tt.from, tt.action)
			assert.Error(t, err)
		})
	}
}

func TestLegalDispatchTransitions(t *testing.T) {
	tests := []struct {
		name     string
		from     DispatchStatus
		action   string
		expected DispatchStatus
	}{
		{"pending_to_delivered", DispatchPending, "deliver", DispatchDelivered},
		{"delivered_to_acked", DispatchDelivered, "ack", DispatchAcked},
		{"acked_to_processing", DispatchAcked, "start", DispatchProcessing},
		{"processing_to_succeeded", DispatchProcessing, "succeed", DispatchSucceeded},
		{"processing_to_failed", DispatchProcessing, "fail", DispatchFailed},
		{"delivered_to_timed_out", DispatchDelivered, "timeout", DispatchTimedOut},
		{"timed_out_to_pending", DispatchTimedOut, "redeliver", DispatchPending},
		{"failed_to_dead_letter", DispatchFailed, "dead_letter", DispatchDeadLetter},
		{"failed_to_retry", DispatchFailed, "retry", DispatchPending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DispatchTransition(tt.from, tt.action)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestTerminalStates(t *testing.T) {
	assert.True(t, IsTerminal(StatusCompleted))
	assert.True(t, IsTerminal(StatusRevoked))
	assert.True(t, IsTerminal(StatusRolledBack))
	assert.False(t, IsTerminal(StatusDispatching))
	assert.False(t, IsTerminal(StatusPartialFailed))
	assert.True(t, DispatchIsTerminal(DispatchSucceeded))
	assert.True(t, DispatchIsTerminal(DispatchDeadLetter))
	assert.False(t, DispatchIsTerminal(DispatchPending))
	assert.False(t, DispatchIsTerminal(DispatchProcessing))
}

func TestRevokeFromAnyNonTerminalState(t *testing.T) {
	nonTerminal := []Status{
		StatusDraft, StatusSubmitted, StatusDispatching,
		StatusPartialSuccess, StatusPartialFailed, StatusCompensating,
	}
	for _, s := range nonTerminal {
		ok := CanTransition(s, ActionRevoke)
		assert.True(t, ok, "revoke should be allowed from %s", s)
	}
}

func TestCanTransitionChecksLegality(t *testing.T) {
	assert.True(t, CanTransition(StatusDraft, ActionSubmit))
	assert.False(t, CanTransition(StatusCompleted, ActionSubmit))
	assert.True(t, CanTransition(StatusDispatching, ActionRevoke))
	assert.False(t, CanTransition(StatusRevoked, ActionDispatch))
}

func TestLegalActionsReturnsValidSet(t *testing.T) {
	actions := LegalActions(StatusDispatching)
	assert.NotEmpty(t, actions)
	assert.Contains(t, actions, ActionAckAll)
	assert.Contains(t, actions, ActionAckPartial)
	assert.Contains(t, actions, ActionFailPartial)
	assert.Contains(t, actions, ActionRevoke)
}

func TestChangeValidation(t *testing.T) {
	tests := []struct {
		name    string
		change  Change
		wantErr bool
	}{
		{"valid", Change{EnterpriseID: "ent-1", ChangeType: TypeLegalRepresentative, NewValue: "张三", SubmittedBy: "clerk1", EventTime: time.Now()}, false},
		{"missing_enterprise", Change{ChangeType: TypeLegalRepresentative, NewValue: "张三", SubmittedBy: "clerk1"}, true},
		{"invalid_change_type", Change{EnterpriseID: "ent-1", ChangeType: "invalid", NewValue: "张三", SubmittedBy: "clerk1"}, true},
		{"missing_new_value", Change{EnterpriseID: "ent-1", ChangeType: TypeLegalRepresentative, SubmittedBy: "clerk1"}, true},
		{"missing_submitted_by", Change{EnterpriseID: "ent-1", ChangeType: TypeLegalRepresentative, NewValue: "张三"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.change.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
