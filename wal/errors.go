package wal

import "errors"

var (
	ErrReplicationSlotActive = errors.New("replication slot is active")

	ErrPublicationEmpty     = errors.New("publication is empty")
	ErrReplicationSlotEmpty = errors.New("replication slot is empty")

	ErrConnectionStrReplicationDatabaseParamAbsent = errors.New("connection string 'replication=database' param is absent")

	ErrTableNotFound = errors.New("table does not exist")
)
