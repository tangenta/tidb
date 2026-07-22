package variable

type pkdbSessionVars struct {
	// CreateFromSelectUsingImport indicates whether to use import into to create table as select.
	CreateFromSelectUsingImport bool

	// PlanCacheMaxDecimalParamNums indicates the max number of decimal parameters which can use the plan cache
	PlanCacheMaxDecimalParamNums int

	// LogHistory indicate historical login information
	LogHistory string
}
