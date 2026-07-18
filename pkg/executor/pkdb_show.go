package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

func getSystemSessionVarValues(sessionVars *variable.SessionVars) (map[string]string, error) {
	values := make(map[string]string, len(systemSessionStateVarNames))
	for _, varName := range systemSessionStateVarNames {
		value, err := sessionVars.GetSessionOrGlobalSystemVar(context.Background(), varName)
		if err != nil {
			return nil, errors.Errorf("can not find %s", varName)
		}
		values[varName] = value
	}
	return values, nil
}

func setSystemSessionVarValues(sessionVars *variable.SessionVars, values map[string]string) error {
	for _, varName := range systemSessionStateVarNames {
		if err := sessionVars.SetSystemVar(varName, values[varName]); err != nil {
			return err
		}
	}
	return nil
}
