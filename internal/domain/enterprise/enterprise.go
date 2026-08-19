package enterprise

import (
	"encoding/json"
	"fmt"
	"time"
)

// Enterprise is the core business entity — a registered company whose
// registration details can be changed and dispatched to departments.
type Enterprise struct {
	ID                  string    `json:"id"`
	Name                string    `json:"name"`
	LegalRepresentative string    `json:"legal_representative"`
	UnifiedCreditCode   string    `json:"unified_credit_code"`
	RegisteredCapital   string    `json:"registered_capital"`
	BusinessScope       string    `json:"business_scope"`
	IndustryCode        string    `json:"industry_code"`
	Status              string    `json:"status"`
	Version             int       `json:"version"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// Snapshot captures the state of an enterprise at a point in time, used as
// the before/after context for a registration change.
type Snapshot struct {
	Name                string `json:"name"`
	LegalRepresentative string `json:"legal_representative"`
	RegisteredCapital   string `json:"registered_capital"`
	BusinessScope       string `json:"business_scope"`
}

// ToSnapshot extracts the current mutable fields into a Snapshot.
func (e *Enterprise) ToSnapshot() Snapshot {
	return Snapshot{
		Name:                e.Name,
		LegalRepresentative: e.LegalRepresentative,
		RegisteredCapital:   e.RegisteredCapital,
		BusinessScope:       e.BusinessScope,
	}
}

// ApplyChange mutates the enterprise according to a change type and target
// value. It returns an error if the change type is not recognised.
func (e *Enterprise) ApplyChange(changeType, newValue string) error {
	switch changeType {
	case "legal_representative":
		e.LegalRepresentative = newValue
	case "name":
		e.Name = newValue
	case "registered_capital":
		e.RegisteredCapital = newValue
	case "business_scope":
		e.BusinessScope = newValue
	default:
		return fmt.Errorf("unsupported change type: %s", changeType)
	}
	e.Version++
	return nil
}

// SnapshotJSON serialises a Snapshot to JSON for persistence.
func SnapshotJSON(s Snapshot) (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("marshal snapshot: %w", err)
	}
	return string(b), nil
}

// ParseSnapshot deserialises a Snapshot from JSON.
func ParseSnapshot(raw string) (Snapshot, error) {
	var s Snapshot
	if raw == "" {
		return s, nil
	}
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return Snapshot{}, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return s, nil
}

// Validate checks that required fields are present.
func (e *Enterprise) Validate() error {
	if e.Name == "" {
		return fmt.Errorf("enterprise name is required")
	}
	if e.LegalRepresentative == "" {
		return fmt.Errorf("legal representative is required")
	}
	if e.UnifiedCreditCode == "" {
		return fmt.Errorf("unified credit code is required")
	}
	if len(e.UnifiedCreditCode) != 18 {
		return fmt.Errorf("unified credit code must be 18 characters")
	}
	if e.RegisteredCapital == "" {
		return fmt.Errorf("registered capital is required")
	}
	if e.BusinessScope == "" {
		return fmt.Errorf("business scope is required")
	}
	return nil
}

// StatusActive and StatusRevoked are the enterprise lifecycle states.
const (
	StatusActive  = "active"
	StatusRevoked = "revoked"
)
