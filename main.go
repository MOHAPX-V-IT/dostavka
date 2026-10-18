package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const version = "0.1.0-rc.1"

var errNoPending = errors.New("no pending deliveries")

type Subscription struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	URL    string `json:"url"`
	Secret string `json:"secret,omitempty"`
	Active bool   `json:"active"`
}
type Delivery struct {
	ID           string          `json:"id"`
	Subscription string          `json:"subscription"`
	URL          string          `json:"url"`
	Event        json.RawMessage `json:"event"`
	Status       string          `json:"status"`
	Attempts     int             `json:"attempts"`
	Next         time.Time       `json:"next"`
	LastError    string          `json:"last_error"`
	Created      time.Time       `json:"created"`
}

// PersistentDelivery stores the frozen signing key, even if the subscription changes.
type PersistentDelivery struct {
	Delivery
	SigningKey string `json:"signing_key"`
}
type State struct {
	Subscriptions map[string]Subscription       `json:"subscriptions"`
	Deliveries    map[string]PersistentDelivery `json:"deliveries"`
	Keys          map[string]string             `json:"keys"`
}
type Service struct {
	mu          sync.Mutex
	state       State
	path        string
	client      *http.Client
	token       string
	maxAttempts int
	base        time.Duration
	workers     int
}

func id() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func openService(path, token string, workers int) (*Service, error) {
	s := &Service{path: path, token: token, workers: workers, maxAttempts: 6, base: time.Second, client: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}
	s.state = State{map[string]Subscription{}, map[string]PersistentDelivery{}, map[string]string{}}
	b, e := os.ReadFile(path)
	if e == nil {
		if e = json.Unmarshal(b, &s.state); e != nil {
			return nil, fmt.Errorf("read state: %w", e)
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	if s.state.Subscriptions == nil || s.state.Deliveries == nil || s.state.Keys == nil {
		return nil, errors.New("invalid state maps")
	}
	for k, d := range s.state.Deliveries {
		if d.Status == "inflight" {
			d.Status = "pending"
			d.Next = time.Now()
			d.LastError = "recovered after process restart"
			s.state.Deliveries[k] = d
		}
	}
	if e = s.save(); e != nil {
		return nil, e
	}
	return s, nil
}
func (s *Service) save() error {
	b, e := json.MarshalIndent(s.state, "", "  ")
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(s.path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(s.path), "state-*.tmp")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return os.Rename(tmp, s.path)
}

// mutate persists before exposing success; on disk failure the previous state is restored.
func (s *Service) mutate(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	backup, _ := json.Marshal(s.state)
	if e := fn(); e != nil {
		var restored State
		_ = json.Unmarshal(backup, &restored)
		s.state = restored
		return e
	}
	if e := s.save(); e != nil {
		var restored State
		_ = json.Unmarshal(backup, &restored)
		s.state = restored
		return e
	}
	return nil
}
func signature(secret, timestamp string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(timestamp + "."))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}
func (s *Service) claim() (PersistentDelivery, bool) {
	var got PersistentDelivery
	ok := false
	e := s.mutate(func() error {
		for k, d := range s.state.Deliveries {
			if d.Status == "pending" && !d.Next.After(time.Now()) {
				d.Status = "inflight"
				d.Attempts++
				s.state.Deliveries[k] = d
				got = d
				ok = true
				break
			}
		}
		if !ok {
			return errNoPending
		}
		return nil
	})
	if e != nil {
		if !errors.Is(e, errNoPending) {
			log.Printf("claim persistence: %v", e)
		}
		return got, false
	}
	return got, ok
}
func (s *Service) deliver(ctx context.Context, d PersistentDelivery) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req, e := http.NewRequestWithContext(ctx, "POST", d.URL, strings.NewReader(string(d.Event)))
	if e == nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Dostavka-ID", d.ID)
		req.Header.Set("X-Dostavka-Timestamp", ts)
		req.Header.Set("X-Dostavka-Signature", "sha256="+signature(d.SigningKey, ts, d.Event))
		var r *http.Response
		r, e = s.client.Do(req)
		if e == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 65536))
			r.Body.Close()
			if r.StatusCode < 200 || r.StatusCode >= 300 {
				e = fmt.Errorf("HTTP %d", r.StatusCode)
			}
		}
	}
	err := s.mutate(func() error {
		current := s.state.Deliveries[d.ID]
		if e == nil {
			current.Status = "delivered"
			current.LastError = ""
		} else {
			current.LastError = e.Error()
			if current.Attempts >= s.maxAttempts {
				current.Status = "dead"
			} else {
				current.Status = "pending"
				current.Next = time.Now().Add(s.base * time.Duration(1<<min(current.Attempts-1, 8)))
			}
		}
		s.state.Deliveries[d.ID] = current
		return nil
	})
	if err != nil {
		log.Printf("delivery result persistence failed: %v; restart recovers lease", err)
	}
}
func (s *Service) run(ctx context.Context) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	for n := 0; n < s.workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if d, ok := s.claim(); ok {
					s.deliver(ctx, d)
					continue
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	return wg
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if e := dec.Decode(v); e != nil {
		fail(w, 400, "invalid JSON: "+e.Error())
		return false
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		fail(w, 400, "expected one JSON value")
		return false
	}
	return true
}
func (s *Service) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok", "version": version})
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		fmt.Fprint(w, adminHTML)
	})
	protected := http.NewServeMux()
	protected.HandleFunc("GET /api/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		out := []Subscription{}
		for _, v := range s.state.Subscriptions {
			v.Secret = ""
			out = append(out, v)
		}
		writeJSON(w, 200, out)
	})
	protected.HandleFunc("POST /api/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		var v Subscription
		if !decode(w, r, &v) {
			return
		}
		u, e := url.Parse(v.URL)
		if e != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
			fail(w, 400, "URL must be absolute HTTP(S), without userinfo")
			return
		}
		if len(v.Secret) < 16 || len(v.Name) == 0 || len(v.Name) > 100 {
			fail(w, 400, "name required (max 100); secret must contain at least 16 characters")
			return
		}
		v.ID = id()
		v.Active = true
		e = s.mutate(func() error { s.state.Subscriptions[v.ID] = v; return nil })
		if e != nil {
			fail(w, 503, e.Error())
			return
		}
		v.Secret = ""
		writeJSON(w, 201, v)
	})
	protected.HandleFunc("DELETE /api/subscriptions/{id}", func(w http.ResponseWriter, r *http.Request) {
		e := s.mutate(func() error {
			v, ok := s.state.Subscriptions[r.PathValue("id")]
			if !ok {
				return os.ErrNotExist
			}
			v.Active = false
			s.state.Subscriptions[v.ID] = v
			return nil
		})
		if errors.Is(e, os.ErrNotExist) {
			fail(w, 404, "subscription not found")
			return
		}
		if e != nil {
			fail(w, 503, e.Error())
			return
		}
		w.WriteHeader(204)
	})
	protected.HandleFunc("POST /api/events", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Subscription string          `json:"subscription"`
			Event        json.RawMessage `json:"event"`
		}
		if !decode(w, r, &v) {
			return
		}
		key := r.Header.Get("Idempotency-Key")
		if len(key) < 1 || len(key) > 200 || len(v.Event) == 0 {
			fail(w, 400, "Idempotency-Key (1..200) and event required")
			return
		}
		sum := sha256.Sum256(append([]byte(v.Subscription+"\n"), v.Event...))
		fingerprint := hex.EncodeToString(sum[:])
		var delivery PersistentDelivery
		duplicate := false
		e := s.mutate(func() error {
			stored, ok := s.state.Keys[key]
			if ok {
				parts := strings.SplitN(stored, ":", 2)
				if len(parts) != 2 || parts[1] != fingerprint {
					return errors.New("idempotency key payload conflict")
				}
				delivery = s.state.Deliveries[parts[0]]
				duplicate = true
				return nil
			}
			sub, ok := s.state.Subscriptions[v.Subscription]
			if !ok || !sub.Active {
				return os.ErrNotExist
			}
			delivery = PersistentDelivery{Delivery: Delivery{ID: id(), Subscription: sub.ID, URL: sub.URL, Event: v.Event, Status: "pending", Next: time.Now(), Created: time.Now()}, SigningKey: sub.Secret}
			s.state.Deliveries[delivery.ID] = delivery
			s.state.Keys[key] = delivery.ID + ":" + fingerprint
			return nil
		})
		if errors.Is(e, os.ErrNotExist) {
			fail(w, 404, "active subscription not found")
			return
		}
		if e != nil {
			if e.Error() == "idempotency key payload conflict" {
				fail(w, 409, e.Error())
			} else {
				fail(w, 503, e.Error())
			}
			return
		}
		status := 202
		if duplicate {
			status = 200
		}
		writeJSON(w, status, delivery.Delivery)
	})
	protected.HandleFunc("GET /api/deliveries", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		out := []Delivery{}
		status := r.URL.Query().Get("status")
		for _, d := range s.state.Deliveries {
			if status == "" || d.Status == status {
				out = append(out, d.Delivery)
			}
		}
		writeJSON(w, 200, out)
	})
	protected.HandleFunc("POST /api/deliveries/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
		e := s.mutate(func() error {
			d, ok := s.state.Deliveries[r.PathValue("id")]
			if !ok {
				return os.ErrNotExist
			}
			if d.Status != "dead" {
				return errors.New("only dead deliveries can be retried")
			}
			d.Status = "pending"
			d.Attempts = 0
			d.Next = time.Now()
			d.LastError = ""
			s.state.Deliveries[d.ID] = d
			return nil
		})
		if errors.Is(e, os.ErrNotExist) {
			fail(w, 404, "delivery not found")
			return
		}
		if e != nil {
			fail(w, 409, e.Error())
			return
		}
		w.WriteHeader(204)
	})
	protected.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		counts := map[string]int{}
		for _, d := range s.state.Deliveries {
			counts[d.Status]++
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, status := range []string{"pending", "inflight", "delivered", "dead"} {
			fmt.Fprintf(w, "dostavka_deliveries{status=%q} %d\n", status, counts[status])
		}
	})
	auth := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		if !hmac.Equal([]byte(header), []byte("Bearer "+s.token)) {
			fail(w, 401, "Bearer token required")
			return
		}
		protected.ServeHTTP(w, r)
	})
	mux.Handle("/api/", auth)
	mux.Handle("/metrics", auth)
	return mux
}
func main() {
	addr := flag.String("listen", "127.0.0.1:8787", "listen address (loopback by default)")
	path := flag.String("data", "data/state.json", "durable state file")
	workers := flag.Int("workers", 4, "bounded concurrent deliveries (1..64)")
	allowRemote := flag.Bool("allow-remote", false, "allow management on non-loopback; protect with TLS proxy")
	flag.Parse()
	token := os.Getenv("DOSTAVKA_TOKEN")
	if len(token) < 24 {
		log.Fatal("set DOSTAVKA_TOKEN to at least 24 characters")
	}
	host, _, e := net.SplitHostPort(*addr)
	if e != nil {
		log.Fatal(e)
	}
	ip := net.ParseIP(host)
	if !*allowRemote && (ip == nil || !ip.IsLoopback()) {
		log.Fatal("non-loopback listen requires --allow-remote")
	}
	if *workers < 1 || *workers > 64 {
		log.Fatal("workers must be 1..64")
	}
	s, e := openService(*path, token, *workers)
	if e != nil {
		log.Fatal(e)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	wg := s.run(ctx)
	server := &http.Server{Addr: *addr, Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		stop, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		_ = server.Shutdown(stop)
	}()
	log.Printf("Dostavka %s http://%s", version, *addr)
	if e = server.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
		cancel()
		log.Print(e)
	}
	cancel()
	wg.Wait()
}

//go:embed admin.html
var adminHTML string
