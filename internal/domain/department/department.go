package department

import "fmt"

// Code identifies a承办 department that receives registration changes.
type Code string

const (
	Tax                Code = "tax"                 // 税务
	SocialSecurity     Code = "social_security"     // 社保
	ProvidentFund      Code = "provident_fund"      // 公积金
	IndustrySupervisor Code = "industry_supervisor" // 行业主管
	MarketRegulator    Code = "market_regulator"    // 市场监管
)

// Department describes a承办 department and the change types it handles.
type Department struct {
	Code        Code
	Name        string
	Topic       string
	ChangeTypes []string
}

// All lists every department known to the system.
var All = []Department{
	{
		Code:        Tax,
		Name:        "税务部门",
		Topic:       "topic.tax",
		ChangeTypes: []string{"legal_representative", "registered_capital", "business_scope", "name"},
	},
	{
		Code:        SocialSecurity,
		Name:        "社保部门",
		Topic:       "topic.social_security",
		ChangeTypes: []string{"legal_representative", "name"},
	},
	{
		Code:        ProvidentFund,
		Name:        "公积金中心",
		Topic:       "topic.provident_fund",
		ChangeTypes: []string{"legal_representative", "registered_capital", "name"},
	},
	{
		Code:        IndustrySupervisor,
		Name:        "行业主管部门",
		Topic:       "topic.industry_supervisor",
		ChangeTypes: []string{"business_scope", "legal_representative", "name"},
	},
	{
		Code:        MarketRegulator,
		Name:        "市场监管部门",
		Topic:       "topic.market_regulator",
		ChangeTypes: []string{"name", "registered_capital", "business_scope", "legal_representative"},
	},
}

// ByCode returns the department for the given code or an error if unknown.
func ByCode(c Code) (Department, error) {
	for _, d := range All {
		if d.Code == c {
			return d, nil
		}
	}
	return Department{}, fmt.Errorf("unknown department code: %s", c)
}

// HandlesChangeType returns true if the department is responsible for the
// given change type.
func (d Department) HandlesChangeType(ct string) bool {
	for _, t := range d.ChangeTypes {
		if t == ct {
			return true
		}
	}
	return false
}

// DepartmentsForChange returns all departments that must receive a change of
// the given type. This drives the batch dispatch logic.
func DepartmentsForChange(changeType string) []Department {
	var result []Department
	for _, d := range All {
		if d.HandlesChangeType(changeType) {
			result = append(result, d)
		}
	}
	return result
}

// Codes returns the string codes for a slice of departments.
func Codes(depts []Department) []string {
	codes := make([]string, len(depts))
	for i, d := range depts {
		codes[i] = string(d.Code)
	}
	return codes
}

// IsValidCode returns true if c matches a known department.
func IsValidCode(c string) bool {
	for _, d := range All {
		if string(d.Code) == c {
			return true
		}
	}
	return false
}
