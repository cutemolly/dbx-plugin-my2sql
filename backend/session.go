package main

import (
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
)

// ConnectionSpec is the resolved, secret-bearing description of one saved
// my2sql connection. It is captured from the DBX lifecycle payload on
// connection/connect and reused by every later request for that connection.
//
// Per the DBX plugin contract, the sidecar MUST dial runtime.host:runtime.port
// (the endpoint after all DBX transport layers such as an SSH jump), falling
// back to the connection's own host/port when no transport layer rewrites it.
type ConnectionSpec struct {
	ID          string
	Name        string
	Host        string
	Port        int
	Username    string
	Password    string
	ServerID    int
	Timezone    string
	RuntimeHost string
	RuntimePort int
	// Tls is the connection-form TLS mode: "" / "off" (plain), "require"
	// (encrypt without pinning the certificate) or "verify" (chain + hostname
	// against the system trust store).
	Tls string
}

// TLSConfig maps the form's tls mode onto a crypto/tls config. "require"
// mirrors go-sql-driver's tls=true behaviour (encrypted transport, certificate
// not verified); "verify" checks the chain and the stored host name against
// the system roots. Returns nil for plain mode, never a zero-value config.
func (c *ConnectionSpec) TLSConfig() (*tls.Config, error) {
	switch strings.ToLower(strings.TrimSpace(c.Tls)) {
	case "", "off", "false", "none":
		return nil, nil
	case "require":
		// #nosec G402 -- InsecureSkipVerify is the documented user choice here,
		// identical to MySQL's `tls=true` and upstream my2sql's --use-ssl.
		return &tls.Config{InsecureSkipVerify: true}, nil
	case "verify", "verify-ca", "strict":
		// Verify against system roots. ServerName is only meaningful for direct
		// TCP; over a DBX tunnel the endpoint is localhost and MySQL's
		// certificate rarely matches, which is what "require" is for.
		return &tls.Config{ServerName: c.Host}, nil
	default:
		return nil, fmt.Errorf("unknown tls mode %q: use off, require or verify", c.Tls)
	}
}

// Endpoint returns the address the sidecar should actually connect to.
func (c *ConnectionSpec) Endpoint() string {
	host, port := c.Host, c.Port
	if c.RuntimeHost != "" && c.RuntimePort != 0 {
		host, port = c.RuntimeHost, c.RuntimePort
	}
	return fmt.Sprintf("%s:%d", host, port)
}

// SessionStore keeps the hydrated specs for currently-open plugin connections.
// Plugins own their internal session registries (DBX tears pools down on
// replace/rollback/uninstall), so this is intentionally a simple in-memory map.
type SessionStore struct {
	mu    sync.RWMutex
	specs map[string]*ConnectionSpec
}

func NewSessionStore() *SessionStore {
	return &SessionStore{specs: make(map[string]*ConnectionSpec)}
}

func (s *SessionStore) Put(spec *ConnectionSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.specs[spec.ID] = spec
}

func (s *SessionStore) Get(id string) (*ConnectionSpec, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	spec, ok := s.specs[id]
	return spec, ok
}

// Require returns the spec or an error naming the connection, mirroring DBX's
// own "Connection is not open" behavior for saved-but-disconnected targets.
func (s *SessionStore) Require(id string) (*ConnectionSpec, error) {
	if id == "" {
		return nil, fmt.Errorf("missing connectionId")
	}
	spec, ok := s.Get(id)
	if !ok {
		return nil, fmt.Errorf("connection is not open: %s - connect the saved my2sql connection first", id)
	}
	return spec, nil
}

func (s *SessionStore) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.specs, id)
}
