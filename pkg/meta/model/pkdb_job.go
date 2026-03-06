package model

// Only used in PingkaiDB.
const (
	ActionCreateTrigger   ActionType = 100
	ActionDropTrigger     ActionType = 101
	ActionCreateProcedure ActionType = 102
	ActionDropProcedure   ActionType = 103
	ActionAlterProcedure  ActionType = 104
)

func init() {
	ActionMap[ActionCreateTrigger] = "create trigger"
	ActionMap[ActionDropTrigger] = "drop trigger"
	ActionMap[ActionCreateProcedure] = "create procedure"
	ActionMap[ActionDropProcedure] = "drop procedure"
	ActionMap[ActionAlterProcedure] = "alter procedure"
}
