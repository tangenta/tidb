package errno

const (
	// ErrForeignKeyWithCreateAsSelect is the error code for unsupported foreign keys in CREATE TABLE ... SELECT.
	ErrForeignKeyWithCreateAsSelect = 3978

	// ErrRowLabelUnAccessible is the error code for inaccessible row security labels.
	ErrRowLabelUnAccessible = 8800
	// ErrInvalidValueOfType is the error code for invalid enterprise data type values.
	ErrInvalidValueOfType = 8801

	// ErrLBACInvalid is the error code for invalid LBAC definitions.
	ErrLBACInvalid = 8802
	// ErrLBACNotFound is the error code for missing LBAC objects.
	ErrLBACNotFound = 8803
	// ErrLBACAlreadyExists is the error code for duplicate LBAC objects.
	ErrLBACAlreadyExists = 8804

	// ErrProcedureDisabled is the error code returned when stored procedures are disabled.
	// Error codes 8805 through 8809 are reserved for tablegroup errors.
	ErrProcedureDisabled = 8810
)
