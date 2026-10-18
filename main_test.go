package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testToken = "012345678901234567890123456789"

func request(t *testing.T, s *Service, method, path, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set("Idempotency-Key", key)
	w := httptest.NewRecorder()
	s.handler().ServeHTTP(w, r)
	return w
}
func service(t *testing.T) *Service {
	t.Helper()
	s, e := openService(filepath.Join(t.TempDir(), "state.json"), testToken, 2)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func subscribe(t *testing.T, s *Service, target string) string {
	t.Helper()
	w := request(t, s, "POST", "/api/subscriptions", fmt.Sprintf(`{"name":"demo","url":%q,"secret":"1234567890123456"}`, target), "")
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	var v Subscription
	_ = json.Unmarshal(w.Body.Bytes(), &v)
	return v.ID
}
func TestSignedDeliveryDedupRestart(t *testing.T) {
	ch := make(chan string, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		want := "sha256=" + signature("1234567890123456", r.Header.Get("X-Dostavka-Timestamp"), b)
		if r.Header.Get("X-Dostavka-Signature") != want {
			t.Error("signature mismatch")
		}
		ch <- r.Header.Get("X-Dostavka-ID")
		w.WriteHeader(204)
	}))
	defer receiver.Close()
	s := service(t)
	sub := subscribe(t, s, receiver.URL)
	body := fmt.Sprintf(`{"subscription":%q,"event":{"hello":"world"}}`, sub)
	w := request(t, s, "POST", "/api/events", body, "key-1")
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	if request(t, s, "POST", "/api/events", body, "key-1").Code != 200 {
		t.Fatal("dedup failed")
	}
	if request(t, s, "POST", "/api/events", strings.Replace(body, "world", "different", 1), "key-1").Code != 409 {
		t.Fatal("conflict not rejected")
	}
	recovered, e := openService(s.path, testToken, 1)
	if e != nil {
		t.Fatal(e)
	}
	d, ok := recovered.claim()
	if !ok {
		t.Fatal("queue lost on restart")
	}
	recovered.deliver(context.Background(), d)
	select {
	case got := <-ch:
		if got != d.ID {
			t.Fatal("delivery ID")
		}
	case <-time.After(time.Second):
		t.Fatal("not delivered")
	}
	again, e := openService(s.path, testToken, 1)
	if e != nil {
		t.Fatal(e)
	}
	if again.state.Deliveries[d.ID].Status != "delivered" {
		t.Fatal("result not durable")
	}
	if len(again.state.Deliveries) != 1 {
		t.Fatal("duplicate persisted")
	}
}
func TestDeadLetterManualRetryAndLease(t *testing.T) {
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer receiver.Close()
	s := service(t)
	s.maxAttempts = 1
	sub := subscribe(t, s, receiver.URL)
	w := request(t, s, "POST", "/api/events", fmt.Sprintf(`{"subscription":%q,"event":{}}`, sub), "k")
	if w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	d, _ := s.claim()
	s.deliver(context.Background(), d)
	if s.state.Deliveries[d.ID].Status != "dead" {
		t.Fatal("not dead")
	}
	if request(t, s, "POST", "/api/deliveries/"+d.ID+"/retry", "", "").Code != 204 {
		t.Fatal("retry failed")
	}
	d, _ = s.claim()
	restarted, e := openService(s.path, testToken, 1)
	if e != nil {
		t.Fatal(e)
	}
	if restarted.state.Deliveries[d.ID].Status != "pending" {
		t.Fatal("inflight lease not recovered")
	}
}
func TestAuthAndValidation(t *testing.T) {
	s := service(t)
	w := httptest.NewRecorder()
	s.handler().ServeHTTP(w, httptest.NewRequest("GET", "/api/deliveries", nil))
	if w.Code != 401 {
		t.Fatal("auth bypass")
	}
	if request(t, s, "POST", "/api/subscriptions", `{"name":"x","url":"file:///etc/passwd","secret":"1234567890123456"}`, "").Code != 400 {
		t.Fatal("URL validation")
	}
	if request(t, s, "POST", "/api/events", `{} {}`, "x").Code != 400 {
		t.Fatal("trailing JSON")
	}
}

func TestPersistenceFailureRollback(t *testing.T) {
	s := service(t)
	original := s.path
	bytesBefore, _ := os.ReadFile(original)
	s.path = filepath.Dir(original) // rename over a directory must fail
	w := request(t, s, "POST", "/api/subscriptions", `{"name":"test","url":"https://example.com/hook","secret":"1234567890123456"}`, "")
	if w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
	if len(s.state.Subscriptions) != 0 {
		t.Fatal("failed write leaked into memory")
	}
	bytesAfter, _ := os.ReadFile(original)
	if string(bytesBefore) != string(bytesAfter) {
		t.Fatal("prior state modified")
	}
}

func TestBoundedWorkersAndRetry(t *testing.T) {
	var active, peak, calls atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		call := calls.Add(1)
		time.Sleep(15 * time.Millisecond)
		if call == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(204)
		}
	}))
	defer receiver.Close()
	s := service(t)
	s.base = time.Millisecond
	sub := subscribe(t, s, receiver.URL)
	for i := 0; i < 6; i++ {
		w := request(t, s, "POST", "/api/events", fmt.Sprintf(`{"subscription":%q,"event":{"i":%d}}`, sub, i), fmt.Sprint(i))
		if w.Code != 202 {
			t.Fatal(w.Body.String())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	wg := s.run(ctx)
	defer func() { cancel(); wg.Wait() }()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		delivered := 0
		for _, d := range s.state.Deliveries {
			if d.Status == "delivered" {
				delivered++
			}
		}
		s.mu.Unlock()
		if delivered == 6 {
			if peak.Load() > 2 {
				t.Fatal("worker bound exceeded")
			}
			if calls.Load() != 7 {
				t.Fatal("failed attempt not retried")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("workers did not drain queue")
}
