module github.com/dbx-plugins/my2sql

go 1.22

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/siddontang/go-log v0.0.0-20180807004314-8d05993dda07
	github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk v0.0.0
)

require (
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/pingcap/errors v0.11.5-0.20221009092201-b66cddb77c32 // indirect
	github.com/pingcap/log v1.1.1-0.20230317032135-a0d097d16e22 // indirect
	github.com/pingcap/tidb/pkg/parser v0.0.0-20231103042308-035ad5ccbe67 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.26.0 // indirect
	golang.org/x/exp v0.0.0-20231006140011-7918f672742d // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/go-mysql-org/go-mysql v1.9.1
	github.com/google/uuid v1.6.0
	golang.org/x/text v0.14.0
)

replace github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk => ./dbx-plugin-sdk
