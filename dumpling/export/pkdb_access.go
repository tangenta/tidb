// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package export

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

type dumpTableList struct {
	tableNames string
}

type userGrants struct {
	defaultRoleName string
	privilegesLists string
}

type userInfo struct {
	userName string
	host     string
}

type accessMeta struct {
	dumpStartTime time.Time
	user          *userInfo
	dumpTableList *dumpTableList
	grants        *userGrants
	where         string
	dumpEndTime   time.Time
	extStore      storeapi.Storage
}

func getTableList(conf *Config) *dumpTableList {
	var sb strings.Builder
	for db, tabs := range conf.Tables {
		if len(tabs) == 0 {
			sb.WriteString(db)
			sb.WriteString(" \n")
			continue
		}
		for _, tab := range tabs {
			sb.WriteString(db)
			sb.WriteString(".")
			sb.WriteString(tab.Name)
			sb.WriteString(" ")
		}
		sb.WriteString("\n")
	}
	return &dumpTableList{tableNames: sb.String()}
}

func getSimpleQueryResult(ctx context.Context, sql string, db *sql.Conn) ([]string, error) {
	var res []string
	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	for rows.Next() {
		var rowStr string
		err = rows.Scan(&rowStr)
		if err != nil {
			return res, err
		}
		res = append(res, rowStr)
	}
	return res, rows.Err()
}

func strConcat(str []string, defaultString string) string {
	if len(str) == 0 {
		return defaultString
	}
	return strings.Join(str, "\n")
}

func (am *accessMeta) getUserGrants(ctx context.Context, db *sql.Conn) error {
	defaultRoles, err := getSimpleQueryResult(ctx, "SELECT CURRENT_ROLE();", db)
	if err != nil {
		return err
	}
	defaultRoleStr := strConcat(defaultRoles, "NONE")

	username, err := getSimpleQueryResult(ctx, "SELECT USER();", db)
	if err != nil {
		return err
	}
	usernameStr := strConcat(username, "NULL")
	ss := strings.Split(usernameStr, "@")
	if len(ss) != 2 {
		am.user.userName = "NULL"
		am.user.host = "NULL"
	} else {
		am.user.host = ss[1]
	}

	grants, err := getSimpleQueryResult(ctx, "SHOW GRANTS;", db)
	if err != nil {
		return err
	}
	grantStr := strConcat(grants, "NULL")

	am.grants = &userGrants{
		defaultRoleName: defaultRoleStr,
		privilegesLists: grantStr,
	}
	return nil
}

func (am *accessMeta) setDumpEndTime() {
	am.dumpEndTime = time.Now()
}

func newAccessMeta(conf *Config, extStore storeapi.Storage) *accessMeta {
	user := &userInfo{
		userName: conf.User,
		host:     conf.Host,
	}
	tables := getTableList(conf)
	return &accessMeta{
		user:          user,
		where:         conf.Where,
		dumpTableList: tables,
		dumpStartTime: time.Now(),
		extStore:      extStore,
	}
}

func (am *accessMeta) formatPrint() string {
	var sb strings.Builder

	sb.WriteString("dump task start time: ")
	sb.WriteString(am.dumpStartTime.String())
	sb.WriteString("\n")

	sb.WriteString("dump task end time: ")
	sb.WriteString(am.dumpEndTime.String())
	sb.WriteString("\n")

	sb.WriteString("\nuser info: ")
	sb.WriteString(am.user.userName)
	sb.WriteString("@")
	sb.WriteString(am.user.host)
	sb.WriteString("\n")

	sb.WriteString("role info: ")
	sb.WriteString(am.grants.defaultRoleName)
	sb.WriteString("\n")

	sb.WriteString("privileges info: \n")
	sb.WriteString(am.grants.privilegesLists)
	sb.WriteString("\n")

	sb.WriteString("\ndump table info: \n")
	sb.WriteString(am.dumpTableList.tableNames)
	sb.WriteString("\n")

	if len(am.where) > 0 {
		sb.WriteString("dump data conditions: ")
		sb.WriteString(am.where)
		sb.WriteString("\n")
	}

	return sb.String()
}

func (am *accessMeta) writeAccessMeta(ctx context.Context) error {
	accessMetaStr := am.formatPrint()
	w, err := am.extStore.Create(ctx, "accessmeta", nil)
	if err != nil {
		return err
	}
	_, err = w.Write(ctx, []byte(accessMetaStr))
	if err != nil {
		return err
	}
	return w.Close(ctx)
}
