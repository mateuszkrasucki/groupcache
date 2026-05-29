/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package groupcache

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	peerAddrs  = flag.String("test_peer_addrs", "", "Comma-separated list of peer addresses; used by TestHTTPPool")
	peerIndex  = flag.Int("test_peer_index", -1, "Index of which peer this child is; used by TestHTTPPool")
	peerChild  = flag.Bool("test_peer_child", false, "True if running as a child process; used by TestHTTPPool")
	serverAddr = flag.String("test_server_addr", "", "Address of the server Child Getters will hit ; used by TestHTTPPool")
)

func TestHTTPPool(t *testing.T) {
	if *peerChild {
		beChildForTestHTTPPool(t)
		os.Exit(0)
	}

	const (
		nChild = 4
		nGets  = 100
	)

	var serverHits int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello")
		serverHits++
	}))
	defer ts.Close()

	var childAddr []string
	for i := 0; i < nChild; i++ {
		childAddr = append(childAddr, pickFreeAddr(t))
	}

	var cmds []*exec.Cmd
	var wg sync.WaitGroup
	for i := 0; i < nChild; i++ {
		cmd := exec.Command(os.Args[0],
			"--test.run=TestHTTPPool",
			"--test_peer_child",
			"--test_peer_addrs="+strings.Join(childAddr, ","),
			"--test_peer_index="+strconv.Itoa(i),
			"--test_server_addr="+ts.URL,
		)
		cmds = append(cmds, cmd)
		cmd.Stdout = os.Stdout
		wg.Add(1)
		if err := cmd.Start(); err != nil {
			t.Fatal("failed to start child process: ", err)
		}
		go awaitAddrReady(t, childAddr[i], &wg)
	}
	defer func() {
		for i := 0; i < nChild; i++ {
			if cmds[i].Process != nil {
				cmds[i].Process.Kill()
			}
		}
	}()
	wg.Wait()

	// Use a dummy self address so that we don't handle gets in-process.
	self := "should-be-ignored"
	p := NewHTTPPool(self)
	p.Set(addrToURL(childAddr)...)

	// Dummy getter function. Gets should normally go to children only; the
	// parent only sees a local get when a remote call falls through (for
	// example, an ErrRemoteCall from the child after the retry budget is
	// exhausted). For the special keys below we mirror the child's response
	// so the assertions remain meaningful after fall-through.
	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		switch key {
		case "IReturnErrRemoteCall":
			return &ErrRemoteCall{Msg: "I am a ErrRemoteCall error"}
		case "IReturnInternalError":
			return errors.New("I am a errors.New() error")
		}
		return errors.New("parent getter called; something's wrong")
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	g := NewGroup("httpPoolTest", 1<<20, getter)

	for _, key := range testKeys(nGets) {
		var value string
		if err := g.Get(ctx, key, StringSink(&value)); err != nil {
			t.Fatal(err)
		}
		if suffix := ":" + key; !strings.HasSuffix(value, suffix) {
			t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
		}
		t.Logf("Get key=%q, value=%q (peer:key)", key, value)
	}

	if serverHits != nGets {
		t.Error("expected serverHits to equal nGets")
	}
	serverHits = 0

	var value string
	var key = "removeTestKey"

	// Multiple gets on the same key
	for i := 0; i < 2; i++ {
		if err := g.Get(ctx, key, StringSink(&value)); err != nil {
			t.Fatal(err)
		}
	}

	// Should result in only 1 server get
	if serverHits != 1 {
		t.Error("expected serverHits to be '1'")
	}

	// Remove the key from the cache and we should see another server hit
	if err := g.Remove(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Get the key again
	if err := g.Get(ctx, key, StringSink(&value)); err != nil {
		t.Fatal(err)
	}

	// Should register another server get
	if serverHits != 2 {
		t.Error("expected serverHits to be '2'")
	}

	key = "setMyTestKey"
	setValue := []byte("test set")
	// Add the key to the cache, optionally updating our local hot cache
	if err := g.Set(ctx, key, setValue, time.Time{}, false); err != nil {
		t.Fatal(err)
	}

	// Get the key
	var getValue ByteView
	if err := g.Get(ctx, key, ByteViewSink(&getValue)); err != nil {
		t.Fatal(err)
	}

	if serverHits != 2 {
		t.Errorf("expected serverHits to be '3' got '%d'", serverHits)
	}

	if !bytes.Equal(setValue, getValue.ByteSlice()) {
		t.Fatal(errors.New(fmt.Sprintf("incorrect value retrieved after set: %s", getValue)))
	}

	// Key with non-URL characters to test URL encoding roundtrip
	key = "a b/c,d"
	if err := g.Get(ctx, key, StringSink(&value)); err != nil {
		t.Fatal(err)
	}
	if suffix := ":" + key; !strings.HasSuffix(value, suffix) {
		t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
	}

	// Get a key that does not exist
	err := g.Get(ctx, "IReturnErrNotFound", StringSink(&value))
	errNotFound := &ErrNotFound{}
	if !errors.As(err, &errNotFound) {
		t.Fatal(errors.New("expected error to be 'ErrNotFound'"))
	}
	assert.Equal(t, "I am a ErrNotFound error", errNotFound.Error())

	// Get a key that is guaranteed to return a remote error.
	err = g.Get(ctx, "IReturnErrRemoteCall", StringSink(&value))
	errRemoteCall := &ErrRemoteCall{}
	if !errors.As(err, &errRemoteCall) {
		t.Fatalf("expected error to be 'ErrRemoteCall', got '%v'", err)
	}
	assert.Equal(t, "I am a ErrRemoteCall error", errRemoteCall.Error())

	// Get a key that is guaranteed to return an internal (500) error
	err = g.Get(ctx, "IReturnInternalError", StringSink(&value))
	assert.Equal(t, "I am a errors.New() error", err.Error())

	testHTTPPoolSet(t, p, self)
}

func testKeys(n int) (keys []string) {
	keys = make([]string, n)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return
}

func beChildForTestHTTPPool(t *testing.T) {
	addrs := strings.Split(*peerAddrs, ",")

	p := NewHTTPPool("http://" + addrs[*peerIndex])
	p.Set(addrToURL(addrs)...)

	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		if key == "IReturnErrNotFound" {
			return &ErrNotFound{Msg: "I am a ErrNotFound error"}
		}

		if key == "IReturnErrRemoteCall" {
			return &ErrRemoteCall{Msg: "I am a ErrRemoteCall error"}
		}

		if key == "IReturnInternalError" {
			return errors.New("I am a errors.New() error")
		}

		if _, err := http.Get(*serverAddr); err != nil {
			t.Logf("HTTP request from getter failed with '%s'", err)
		}

		dest.SetString(strconv.Itoa(*peerIndex)+":"+key, time.Time{})
		return nil
	})
	NewGroup("httpPoolTest", 1<<20, getter)

	log.Fatal(http.ListenAndServe(addrs[*peerIndex], p))
}

// This is racy. Another process could swoop in and steal the port between the
// call to this function and the next listen call. Should be okay though.
// The proper way would be to pass the l.File() as ExtraFiles to the child
// process, and then close your copy once the child starts.
func pickFreeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func addrToURL(addr []string) []string {
	url := make([]string, len(addr))
	for i := range addr {
		url[i] = "http://" + addr[i]
	}
	return url
}

func awaitAddrReady(t *testing.T, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	const max = 1 * time.Second
	tries := 0
	for {
		tries++
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return
		}
		delay := time.Duration(tries) * 25 * time.Millisecond
		if delay > max {
			delay = max
		}
		time.Sleep(delay)
	}
}

func testHTTPPoolSet(t *testing.T, httpPool *HTTPPool, self string) {
	t.Helper()
	peers := []string{"http://peer1:8080", "http://peer2:8080"}
	peersIncludingSelf := append(peers, self)

	// set on the pool
	httpPool.Set(peersIncludingSelf...)

	// check if returned peer list is correct
	returnedPeers := httpPool.GetPeersList()
	t.Logf("Returned peers: %v", returnedPeers)
	if len(returnedPeers) != len(peersIncludingSelf) {
		t.Fatalf("expected %d peers, got %d", len(peersIncludingSelf), len(returnedPeers))
	}
	if !slices.Equal(peersIncludingSelf, returnedPeers) {
		t.Fatalf("expected peers %v, got %v", peersIncludingSelf, returnedPeers)
	}
	returnedRemotePeers := httpPool.GetRemotePeersList()
	t.Logf("Returned remote peers: %v", returnedRemotePeers)
	if len(returnedRemotePeers) != len(peers) {
		t.Fatalf("expected %d remote peers, got %d", len(peers), len(returnedRemotePeers))
	}
	if !slices.Equal(peers, returnedRemotePeers) {
		t.Fatalf("expected remote peers %v, got %v", peers, returnedRemotePeers)
	}

	// all my getters should be for peers
	peerGetters := httpPool.GetAll()
	for _, getter := range peerGetters {
		t.Logf("Getter peer returned: %s", getter.GetURL())
		httpGetter, ok := getter.(*httpGetter)
		if !ok {
			t.Fatalf("expected getter to be of type *httpGetter, got %T", getter)
		}
		peer := httpGetter.GetPeer()
		if !slices.Contains(peers, peer) {
			t.Fatalf("expected getter URL %s to be in peers list %v", peer, peers)
		}
	}

	// now I will set again with peer1 removed, and peer3 added
	newPeers := []string{"http://peer2:8080", "http://peer3:8080"}
	newPeersIncludingSelf := append(newPeers, self)
	httpPool.Set(newPeersIncludingSelf...)

	// check if returned peer list is correct
	returnedPeers = httpPool.GetPeersList()
	t.Logf("Returned peers after update: %v", returnedPeers)
	if len(returnedPeers) != len(newPeersIncludingSelf) {
		t.Fatalf("expected %d peers, got %d", len(newPeersIncludingSelf), len(returnedPeers))
	}
	if !slices.Equal(newPeersIncludingSelf, returnedPeers) {
		t.Fatalf("expected peers %v, got %v", newPeersIncludingSelf, returnedPeers)
	}
	returnedRemotePeers = httpPool.GetRemotePeersList()
	t.Logf("Returned remote peers after update: %v", returnedRemotePeers)
	if len(returnedRemotePeers) != len(newPeers) {
		t.Fatalf("expected %d remote peers, got %d", len(newPeers), len(returnedRemotePeers))
	}
	if !slices.Equal(newPeers, returnedRemotePeers) {
		t.Fatalf("expected remote peers %v, got %v", newPeers, returnedRemotePeers)
	}

	// all  getters should be for peers
	newPeerGetters := httpPool.GetAll()
	for _, getter := range newPeerGetters {
		t.Logf("Getter peer returned after update: %s", getter.GetURL())
		httpGetter, ok := getter.(*httpGetter)
		if !ok {
			t.Fatalf("expected getter to be of type *httpGetter, got %T", getter)
		}
		peer := httpGetter.GetPeer()
		if !slices.Contains(newPeers, peer) {
			t.Fatalf("expected getter URL %s to be in peers list %v", peer, newPeers)
		}
	}

	// the peer1 getter should be closed and should not be in the new getters list
	var peer1GetterBefore, peer1GetterAfter *httpGetter
	for _, getter := range peerGetters {
		httpGetter := getter.(*httpGetter)
		if httpGetter.GetPeer() == "http://peer1:8080" {
			peer1GetterBefore = httpGetter
			select {
			case <-peer1GetterBefore.peerLifetimeCtx.Done():
			default:
				t.Fatal("expected peer1 getter to be closed, but it was not")
			}
			break
		}
	}
	for _, getter := range newPeerGetters {
		httpGetter := getter.(*httpGetter)
		if httpGetter.GetPeer() == "http://peer1:8080" {
			peer1GetterAfter = httpGetter
			break
		}
	}
	if peer1GetterBefore == nil {
		t.Fatal("expected to find peer1 getter in old getters")
	}
	if peer1GetterAfter != nil {
		t.Fatal("expected to not find peer1 getter in new getters")
	}

	// the peer2 getter should not be closed and should be the same instance as before
	var peer2GetterBefore, peer2GetterAfter *httpGetter
	for _, getter := range peerGetters {
		httpGetter := getter.(*httpGetter)
		if httpGetter.GetPeer() == "http://peer2:8080" {
			peer2GetterBefore = httpGetter
			select {
			case <-peer2GetterBefore.peerLifetimeCtx.Done():
				t.Fatal("expected peer2 getter to not be closed, but it was")
			default:
			}
			break
		}
	}
	for _, getter := range newPeerGetters {
		httpGetter := getter.(*httpGetter)
		if httpGetter.GetPeer() == "http://peer2:8080" {
			peer2GetterAfter = httpGetter
			break
		}
	}
	if peer2GetterBefore == nil || peer2GetterAfter == nil {
		t.Fatal("expected to find peer2 getter in both old and new getters")
	}
	if peer2GetterBefore != peer2GetterAfter {
		t.Fatal("expected peer2 getter instance to be the same after Set, but it was not")
	}
}

func TestHttpGetterClosed(t *testing.T) {
	// save & restore process-globals so this test composes with others
	// (TestHTTPPool runs first and registers its own picker; calling
	// RegisterPeerPicker again would panic).
	oldPicker := portPicker
	oldLogger := logger
	t.Cleanup(func() {
		portPicker = oldPicker
		logger = oldLogger
	})
	portPicker = nil

	// let's create test peer picker  with two getters
	getter1Ctx, cancelGetter1 := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelGetter1()
	httpGetter1 := &httpGetter{
		baseURL:            "http://does-not.exist",
		peer:               "http://does-not.exist",
		peerLifetimeCtx:    getter1Ctx,
		peerLifetimeCancel: cancelGetter1,
	}
	getter2Ctx, cancelGetter2 := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelGetter2()
	httpGetter2 := &httpGetter{
		baseURL:            "http://this-too-does-not.exist",
		peer:               "http://this-too-does-not.exist",
		peerLifetimeCtx:    getter2Ctx,
		peerLifetimeCancel: cancelGetter2,
	}
	peerPicker := &testPeerPicker{}
	peerPicker.addPeer(httpGetter1)
	peerPicker.addPeer(httpGetter2)

	// let's use a test logger to capture logs from the http getters and peer picker
	logger = testLogger{tb: t}
	// and register our test peer picker
	RegisterPeerPicker(func() PeerPicker { return peerPicker })

	// let's create a group with a dummy getter
	group := NewGroup("httpGetterTest", 1<<20, GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		return dest.SetString(key, time.Now().Add(time.Second))
	}))

	// let's attempt to get in a go routine
	// but first, let's close the getters without removing it from the peer picker
	httpGetter1.Close()
	httpGetter2.Close()

	errCh := make(chan error, 1)
	valCh := make(chan string, 1)
	go func() {
		var value string
		err := group.Get(context.Background(), "testKey", StringSink(&value))
		if err != nil {
			errCh <- err
		}
		valCh <- value
	}()
	// after 5ms let's remove getter 1 from the peer picker so we attempt using getter2
	<-time.After(5 * time.Millisecond)
	peerPicker.removePeer(httpGetter1)

	select {
	case value := <-valCh:
		t.Logf("Get returned with value: %s", value)
	case err := <-errCh:
		// we expect success, it should use the whole retry time and then fetch locally
		t.Errorf("Get returned with error: %s", err)
	case <-time.After(2 * time.Second):
		// this is unexpected
		t.Fatal("expected Get to return after peerLifetimeCtx was canceled")
	}
}

type testPeerPicker struct {
	peers []*httpGetter
	mu    sync.Mutex
}

func (p *testPeerPicker) PickPeer(key string) (peer ProtoGetter, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.peers) == 0 {
		return nil, false
	}
	return p.peers[0], true
}
func (p *testPeerPicker) GetAll() []ProtoGetter {
	p.mu.Lock()
	defer p.mu.Unlock()
	peers := make([]ProtoGetter, len(p.peers))
	for i, peer := range p.peers {
		peers[i] = peer
	}

	return peers
}

func (p *testPeerPicker) addPeer(peer *httpGetter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = append(p.peers, peer)
}

func (p *testPeerPicker) removePeer(peer *httpGetter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, pg := range p.peers {
		if pg.GetURL() == peer.GetURL() {
			p.peers = append(p.peers[:i], p.peers[i+1:]...)
			pg.Close()
			return
		}
	}
}

type testLogger struct {
	tb     testing.TB
	logger []string
}

func (l testLogger) Error() Logger {
	return l
}

func (l testLogger) Warn() Logger {
	return l
}

func (l testLogger) Info() Logger {
	return l
}

func (l testLogger) Debug() Logger {
	return l
}

func (l testLogger) ErrorField(label string, err error) Logger {
	logged := fmt.Sprintf("err(%s)=%s", label, err)
	l.logger = append(l.logger, logged)
	return l
}

func (l testLogger) StringField(label string, val string) Logger {
	logged := fmt.Sprintf("%s=%s", label, val)
	l.logger = append(l.logger, logged)
	return l
}

func (l testLogger) WithFields(fields map[string]interface{}) Logger {
	for k, v := range fields {
		logged := fmt.Sprintf("%s=%v", k, v)
		l.logger = append(l.logger, logged)
	}
	return l
}

func (l testLogger) Printf(format string, args ...interface{}) {
	logged := append(l.logger, fmt.Sprintf(format, args...))
	l.tb.Log(strings.Join(logged, ", "))
}
