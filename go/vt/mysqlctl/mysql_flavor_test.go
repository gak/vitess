// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"os"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysql"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

type fakeMysqlFlavor struct{}

func (fakeMysqlFlavor) PromoteSlaveCommands() []string                 { return nil }
func (fakeMysqlFlavor) ParseGTID(string) (proto.GTID, error)           { return nil, nil }
func (fakeMysqlFlavor) MakeBinlogEvent(buf []byte) blproto.BinlogEvent { return nil }
func (fakeMysqlFlavor) ParseReplicationPosition(string) (proto.ReplicationPosition, error) {
	return proto.ReplicationPosition{}, nil
}
func (fakeMysqlFlavor) SendBinlogDumpCommand(mysqld *Mysqld, conn *SlaveConnection, startPos proto.ReplicationPosition) error {
	return nil
}
func (fakeMysqlFlavor) WaitMasterPos(mysqld *Mysqld, targetPos proto.ReplicationPosition, waitTimeout time.Duration) error {
	return nil
}
func (fakeMysqlFlavor) MasterPosition(mysqld *Mysqld) (proto.ReplicationPosition, error) {
	return proto.ReplicationPosition{}, nil
}
func (fakeMysqlFlavor) SlaveStatus(mysqld *Mysqld) (*proto.ReplicationStatus, error) { return nil, nil }
func (fakeMysqlFlavor) StartReplicationCommands(params *mysql.ConnectionParams, status *proto.ReplicationStatus) ([]string, error) {
	return nil, nil
}

func TestDefaultMysqlFlavor(t *testing.T) {
	os.Setenv("MYSQL_FLAVOR", "")
	mysqlFlavors = make(map[string]MysqlFlavor)
	mysqlFlavors["only one"] = &fakeMysqlFlavor{}
	want := mysqlFlavors["only one"]

	if got := mysqlFlavor(); got != want {
		t.Errorf("mysqlFlavor() = %#v, want %#v", got, want)
	}
}

func TestMysqlFlavorEnvironmentVariable(t *testing.T) {
	os.Setenv("MYSQL_FLAVOR", "fake flavor")
	mysqlFlavors = make(map[string]MysqlFlavor)
	mysqlFlavors["fake flavor"] = &fakeMysqlFlavor{}
	want := mysqlFlavors["fake flavor"]

	if got := mysqlFlavor(); got != want {
		t.Errorf("mysqlFlavor() = %#v, want %#v", got, want)
	}
}
