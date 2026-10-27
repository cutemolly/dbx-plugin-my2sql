package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"

	sdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
)

const pluginID = "io.github.my2sql"
const pluginVersion = "1.0.1"

var errNoConnectionID = errors.New("missing connection id in lifecycle payload")

// newContext returns a request-scoped context. Per-connection timeouts are
// enforced in the driver config (openDB), so a plain background context is
// enough for administrative queries here.
func newContext() context.Context { return context.Background() }

// my2sqlPlugin implements the SDK Handler. It owns the session registry, the
// analysis job manager and the rollback executor.
type my2sqlPlugin struct {
	sessions *SessionStore
	jobs     *JobManager
	exec     *ExecManager
}

func newPlugin() *my2sqlPlugin {
	sessions := NewSessionStore()
	jobs := NewJobManager(engineFor())
	return &my2sqlPlugin{sessions: sessions, jobs: jobs, exec: NewExecManager(sessions, jobs)}
}

// Handle is the SDK entry point. The SDK calls it straight from its request
// loop with no recover of its own, so a panic in any handler would take the
// whole sidecar down: every running job, every open session and every pending
// confirmation token go with it, and the workbench is left reporting "unknown
// job" for work that is still in flight. A panic becomes this request's error
// instead; the stack still reaches the host through stderr.
func (p *my2sqlPlugin) Handle(rc sdk.RequestContext, method string, params json.RawMessage, emitter *sdk.Emitter) (result any, perr *sdk.PluginError) {
	defer func() {
		if rec := recover(); rec != nil {
			// The panic value and its stack belong to the plugin log (the host
			// forwards stderr), not to the RPC client: a driver or parser panic can
			// carry a DSN, and UI error lines get pasted into issue reports.
			debug.PrintStack()
			log.Printf("panic while handling %s: %v", method, rec)
			result = nil
			perr = sdk.NewError(-32000, fmt.Sprintf("internal error while handling %s (details in the plugin log)", method))
		}
	}()
	return p.handle(rc, method, params, emitter)
}

func (p *my2sqlPlugin) handle(_ sdk.RequestContext, method string, params json.RawMessage, emitter *sdk.Emitter) (any, *sdk.PluginError) {
	publish := func(m string, payload any) { _ = emitter.Event(m, payload) }
	ctx := newContext()

	switch method {
	case "connection/test":
		spec, err := parseSpec(params)
		if err != nil {
			return nil, sdk.NewError(-32602, err.Error())
		}
		if _, err := Precheck(ctx, spec); err != nil {
			return map[string]any{"success": false, "message": err.Error()}, nil
		}
		return map[string]any{"success": true, "message": "Connected and binlog settings readable at " + spec.Endpoint()}, nil

	case "connection/connect":
		spec, err := parseSpec(params)
		if err != nil {
			return nil, sdk.NewError(-32602, err.Error())
		}
		p.sessions.Put(spec)
		_ = emitter.Event("my2sql/connectionChanged", map[string]any{"connectionId": spec.ID, "state": "connected"})
		return map[string]any{"success": true}, nil

	case "connection/disconnect":
		id := connectionIDFromParams(params)
		p.sessions.Remove(id)
		_ = emitter.Event("my2sql/connectionChanged", map[string]any{"connectionId": id, "state": "disconnected"})
		return map[string]any{"success": true}, nil

	case "binlog/precheck":
		spec, perr := p.requireSpec(params)
		if perr != nil {
			return nil, perr
		}
		res, err := Precheck(ctx, spec)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return res, nil

	case "binlog/list":
		spec, perr := p.requireSpec(params)
		if perr != nil {
			return nil, perr
		}
		res, err := ListBinlogFiles(ctx, spec)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return res, nil

	case "meta/databases":
		spec, perr := p.requireSpec(params)
		if perr != nil {
			return nil, perr
		}
		dbs, err := ListDatabases(ctx, spec)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return map[string]any{"databases": dbs}, nil

	case "meta/tables":
		spec, perr := p.requireSpec(params)
		if perr != nil {
			return nil, perr
		}
		var q struct {
			Database string `json:"database"`
		}
		_ = json.Unmarshal(params, &q)
		tables, err := ListTables(ctx, spec, q.Database)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return map[string]any{"tables": tables}, nil

	case "schema/columns":
		spec, perr := p.requireSpec(params)
		if perr != nil {
			return nil, perr
		}
		var q struct {
			Database string `json:"database"`
			Table    string `json:"table"`
		}
		_ = json.Unmarshal(params, &q)
		sch, err := SchemaColumns(ctx, spec, q.Database, q.Table)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return sch, nil

	case "job/start":
		opts, err := decodeOptions(params)
		if err != nil {
			return nil, sdk.NewError(-32602, err.Error())
		}
		spec, derr := p.sessions.Require(opts.SourceConnectionID)
		if derr != nil {
			return nil, sdk.NewError(-32010, derr.Error())
		}
		id, err := p.jobs.Start(opts, spec, publish)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return map[string]any{"jobId": id}, nil

	case "job/stop":
		var q struct {
			JobID string `json:"jobId"`
		}
		_ = json.Unmarshal(params, &q)
		if err := p.jobs.Stop(q.JobID, publish); err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return map[string]any{"success": true}, nil

	case "job/status":
		var q struct {
			JobID string `json:"jobId"`
		}
		_ = json.Unmarshal(params, &q)
		snap, err := p.jobs.Snapshot(q.JobID)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return snap, nil

	case "job/history":
		var q struct {
			Limit int `json:"limit"`
		}
		_ = json.Unmarshal(params, &q)
		list, err := p.jobs.History(q.Limit)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return map[string]any{"jobs": list}, nil

	case "job/delete":
		var q struct {
			JobID string `json:"jobId"`
		}
		_ = json.Unmarshal(params, &q)
		if err := p.jobs.Delete(q.JobID); err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		// Any confirmation issued for this job's files is now meaningless.
		p.exec.ForgetPending(q.JobID)
		log.Printf("deleted job %s and its output", q.JobID)
		return map[string]any{"success": true}, nil

	case "job/output/list":
		var q struct {
			JobID string `json:"jobId"`
		}
		_ = json.Unmarshal(params, &q)
		files, err := p.jobs.ListOutputs(q.JobID)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		dir, _ := jobOutputPath(q.JobID)
		return map[string]any{"files": files, "outDir": dir}, nil

	case "job/output/preview":
		var q struct {
			JobID  string `json:"jobId"`
			File   string `json:"file"`
			Offset int    `json:"offset"`
			Limit  int    `json:"limit"`
		}
		_ = json.Unmarshal(params, &q)
		page, err := p.jobs.Preview(q.JobID, q.File, q.Offset, q.Limit)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return page, nil

	case "exec/prepare":
		var q struct {
			JobID              string `json:"jobId"`
			File               string `json:"file"`
			TargetConnectionID string `json:"targetConnectionId"`
		}
		_ = json.Unmarshal(params, &q)
		res, err := p.exec.Prepare(q.JobID, q.File, q.TargetConnectionID)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return res, nil

	case "exec/start":
		var q struct {
			JobID              string `json:"jobId"`
			File               string `json:"file"`
			TargetConnectionID string `json:"targetConnectionId"`
			ConfirmToken       string `json:"confirmToken"`
		}
		_ = json.Unmarshal(params, &q)
		id, err := p.exec.Start(q.JobID, q.File, q.TargetConnectionID, q.ConfirmToken, publish)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return map[string]any{"executionId": id}, nil

	case "exec/stop":
		var q struct {
			ExecutionID string `json:"executionId"`
		}
		_ = json.Unmarshal(params, &q)
		if err := p.exec.Stop(q.ExecutionID); err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return map[string]any{"success": true}, nil

	case "exec/status":
		var q struct {
			ExecutionID string `json:"executionId"`
		}
		_ = json.Unmarshal(params, &q)
		snap, err := p.exec.Snapshot(q.ExecutionID)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return snap, nil

	default:
		return nil, sdk.MethodNotFound(method)
	}
}

func (p *my2sqlPlugin) requireSpec(params json.RawMessage) (spec *ConnectionSpec, perr *sdk.PluginError) {
	var q struct {
		ConnectionID string `json:"connectionId"`
	}
	_ = json.Unmarshal(params, &q)
	s, err := p.sessions.Require(q.ConnectionID)
	if err != nil {
		return nil, sdk.NewError(-32010, err.Error())
	}
	return s, nil
}

// parseSpec builds a ConnectionSpec from a provider lifecycle payload:
// { provider, connection, runtime }. The password arrives already hydrated by
// DBX and never round-trips through the workbench UI.
func parseSpec(params json.RawMessage) (*ConnectionSpec, error) {
	var payload struct {
		Connection struct {
			ID           string          `json:"id"`
			Name         string          `json:"name"`
			Host         string          `json:"host"`
			Port         json.Number     `json:"port"`
			Username     string          `json:"username"`
			Password     string          `json:"password"`
			ExternalJSON json.RawMessage `json:"-"`
			External     map[string]any  `json:"external_config"`
		} `json:"connection"`
		Runtime struct {
			Host string      `json:"host"`
			Port json.Number `json:"port"`
		} `json:"runtime"`
	}
	if err := json.Unmarshal(params, &payload); err != nil {
		return nil, err
	}
	c := payload.Connection
	if c.ID == "" {
		return nil, errNoConnectionID
	}
	port := atoiNum(c.Port, 3306)
	spec := &ConnectionSpec{
		ID:          c.ID,
		Name:        c.Name,
		Host:        c.Host,
		Port:        port,
		Username:    c.Username,
		Password:    c.Password,
		RuntimeHost: payload.Runtime.Host,
		RuntimePort: atoiNum(payload.Runtime.Port, 0),
		ServerID:    atoiAny(c.External["server_id"], 9999),
		Timezone:    stringAny(c.External["timezone"], "+08:00"),
		Tls:         stringAny(c.External["tls"], ""),
	}
	return spec, nil
}

func connectionIDFromParams(params json.RawMessage) string {
	var payload struct {
		Connection struct {
			ID string `json:"id"`
		} `json:"connection"`
	}
	_ = json.Unmarshal(params, &payload)
	return payload.Connection.ID
}

func itoa(n int) string { return strconv.Itoa(n) }

func atoiNum(n json.Number, def int) int {
	if v, err := strconv.Atoi(n.String()); err == nil {
		return v
	}
	return def
}

func atoiAny(v any, def int) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case string:
		if n, err := strconv.Atoi(t); err == nil {
			return n
		}
	}
	return def
}

func stringAny(v any, def string) string {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return def
}

func main() {
	metadata := sdk.Metadata{
		ID:           pluginID,
		Version:      pluginVersion,
		Capabilities: []string{"connections", "events"},
	}
	server := sdk.NewServer(metadata, newPlugin())
	if err := server.Serve(); err != nil {
		// stdout is reserved for the protocol; diagnostics go to stderr.
		log.Fatalf("[%s] sidecar exited: %v", pluginID, err)
	}
}
