/*
Copyright 2012 Google Inc.

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

// Package groupcache provides a data loading mechanism with caching
// and de-duplication that works across a set of peer processes.
//
// Each data Get first consults its local cache, otherwise delegates
// to the requested key's canonical owner, which then checks its cache
// or finally gets the data.  In the common case, many concurrent
// cache misses across a set of peers for the same key result in just
// one cache fill.
package groupcache

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/ccpgames/groupcache/v2/groupcachepb"
	"github.com/ccpgames/groupcache/v2/lru"
	"github.com/ccpgames/groupcache/v2/singleflight"
	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	remoteOperationTimeout            = 30 * time.Second
	maxRemoteCallErrorRetryTime       = 250 * time.Millisecond
	remoteCallErrorBackoffInitial     = time.Millisecond
	remoteCallErrorBackoffMaxInterval = 20 * time.Millisecond

	otelAttributePeerURL = attribute.Key("app.groupcache.peer_url")
)

var logger Logger = NoopLogger{}

// SetLogger - this is legacy to provide backwards compatibility with logrus.
func SetLogger(log *logrus.Entry) {
	logger = LogrusLogger{Entry: log}
}

// SetLoggerFromLogger - set the logger to an implementation of the Logger interface
func SetLoggerFromLogger(log Logger) {
	logger = log
}

// A Getter loads data for a key.
type Getter interface {
	// Get returns the value identified by key, populating dest.
	//
	// The returned data must be unversioned. That is, key must
	// uniquely describe the loaded data, without an implicit
	// current time, and without relying on cache expiration
	// mechanisms.
	Get(ctx context.Context, key string, dest Sink) error
}

// A GetterFunc implements Getter with a function.
type GetterFunc func(ctx context.Context, key string, dest Sink) error

func (f GetterFunc) Get(ctx context.Context, key string, dest Sink) error {
	return f(ctx, key, dest)
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)

	initPeerServerOnce sync.Once
	initPeerServer     func()
)

// GetGroup returns the named group previously created with NewGroup or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// GetGroups returns all groups previously created with NewGroup or nil if no
// groups.
func GetGroups() []*Group {
	list := make([]*Group, 0, len(groups))
	mu.RLock()
	for _, group := range groups {
		list = append(list, group)
	}
	mu.RUnlock()
	return list
}

// NewGroup creates a coordinated group-aware Getter from a Getter.
//
// The returned Getter tries (but does not guarantee) to run only one
// Get call at once for a given key across an entire set of peer
// processes. Concurrent callers both in the local process and in
// other processes receive copies of the answer once the original Get
// completes.
//
// The group name must be unique for each getter.
func NewGroup(name string, cacheBytes int64, getter Getter, opts ...GroupOption) *Group {
	return newGroup(name, cacheBytes, getter, nil, opts...)
}

// DeregisterGroup removes group from group pool
func DeregisterGroup(name string) {
	mu.Lock()
	delete(groups, name)
	mu.Unlock()
}

// groupOpts are options that can be set when creating a new group.
type groupOpts struct {
	clampNotOwnedKeyTTL *time.Duration
	clampHotCacheMaxTTL *time.Duration
	disableHotCache     bool
}

// GroupOption is a function that sets an option when creating a new group.
type GroupOption func(*groupOpts)

// WithClampNotOwnedKeyTTL sets the TTL for keys that are not owned by the peer.
// This is used to prevent values served by the peers that do not own the key from being cached for long.
func WithClampNotOwnedKeyTTL(d time.Duration) GroupOption {
	return func(opts *groupOpts) {
		opts.clampNotOwnedKeyTTL = &d
	}
}

// WithClampHotCacheMaxTTL sets the max TTL for keys in the hot cache.
// This is used to prevent values in the hot cache from being cached for too long causing staleness on certain pods.
func WithClampHotCacheMaxTTL(d time.Duration) GroupOption {
	return func(opts *groupOpts) {
		opts.clampHotCacheMaxTTL = &d
	}
}

// WithDisableHotCache disables the hot cache.
// This is used to prevent values in the hot cache from being cached at all.
func WithDisableHotCache() GroupOption {
	return func(opts *groupOpts) {
		opts.disableHotCache = true
	}
}

// If peers is nil, the peerPicker is called via a sync.Once to initialize it.
func newGroup(name string, cacheBytes int64, getter Getter, peers PeerPicker, opts ...GroupOption) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	optsStruct := groupOpts{
		disableHotCache: false,
	}
	for _, opt := range opts {
		opt(&optsStruct)
	}

	mu.Lock()
	defer mu.Unlock()
	initPeerServerOnce.Do(callInitPeerServer)
	if _, dup := groups[name]; dup {
		panic("duplicate registration of group " + name)
	}
	g := &Group{
		name:           name,
		getter:         getter,
		peers:          peers,
		cacheBytes:     cacheBytes,
		loadGroup:      &singleflight.Group{},
		localLoadGroup: &singleflight.Group{},
		setGroup:       &singleflight.Group{},
		removeGroup:    &singleflight.Group{},
		opts:           optsStruct,
	}
	if fn := newGroupHook; fn != nil {
		fn(g)
	}
	groups[name] = g
	return g
}

// newGroupHook, if non-nil, is called right after a new group is created.
var newGroupHook func(*Group)

// RegisterNewGroupHook registers a hook that is run each time
// a group is created.
func RegisterNewGroupHook(fn func(*Group)) {
	if newGroupHook != nil {
		panic("RegisterNewGroupHook called more than once")
	}
	newGroupHook = fn
}

// RegisterServerStart registers a hook that is run when the first
// group is created.
func RegisterServerStart(fn func()) {
	if initPeerServer != nil {
		panic("RegisterServerStart called more than once")
	}
	initPeerServer = fn
}

func callInitPeerServer() {
	if initPeerServer != nil {
		initPeerServer()
	}
}

// A Group is a cache namespace and associated data loaded spread over
// a group of 1 or more machines.
type Group struct {
	name       string
	getter     Getter
	peersOnce  sync.Once
	peers      PeerPicker
	cacheBytes int64 // limit for sum of mainCache and hotCache size

	// mainCache is a cache of the keys for which this process
	// (amongst its peers) is authoritative. That is, this cache
	// contains keys which consistent hash on to this process's
	// peer number.
	mainCache cache

	// hotCache contains keys/values for which this peer is not
	// authoritative (otherwise they would be in mainCache), but
	// are popular enough to warrant mirroring in this process to
	// avoid going over the network to fetch from a peer.  Having
	// a hotCache avoids network hotspotting, where a peer's
	// network card could become the bottleneck on a popular key.
	// This cache is used sparingly to maximize the total number
	// of key/value pairs that can be stored globally.
	hotCache cache

	// loadGroup ensures that each key is only fetched once
	// (either locally or remotely) when requested from local get
	// regardless of the number of concurrent callers.
	loadGroup flightGroup

	// localLoadGroup ensures that each key is only being fetched once
	// locally regardless of the number of concurrent callers.
	localLoadGroup flightGroup

	// setGroup ensures that each added key is only added
	// remotely once regardless of the number of concurrent callers.
	setGroup flightGroup

	// removeGroup ensures that each removed key is only removed
	// remotely once regardless of the number of concurrent callers.
	removeGroup flightGroup

	_ int32 // force Stats to be 8-byte aligned on 32-bit platforms

	// Stats are statistics on the group.
	Stats Stats

	opts groupOpts
}

// flightGroup is defined as an interface which flightgroup.Group
// satisfies.  We define this so that we may test with an alternate
// implementation.
type flightGroup interface {
	Do(ctx context.Context, key string, fn func() (interface{}, error)) (interface{}, error)
	Lock(fn func())
}

// Stats are per-group statistics.
type Stats struct {
	Gets                     AtomicInt // any Get request, including from peers
	CacheHits                AtomicInt // either cache was good
	GetFromPeersLatencyLower AtomicInt // slowest duration to request value from peers
	PeerLoads                AtomicInt // either remote load or remote cache hit (not an error)
	PeerErrors               AtomicInt
	Loads                    AtomicInt // (gets - cacheHits)
	LoadsDeduped             AtomicInt // after singleflight
	LocalLoads               AtomicInt // local loads
	LocalLoadsDeduped        AtomicInt // after singleflight for local loads
	SuccessfulLocalLoads     AtomicInt // total good local loads
	LocalLoadErrs            AtomicInt // total bad local loads
	ServerRequests           AtomicInt // gets that came over the network from peers
}

// Name returns the name of the group.
func (g *Group) Name() string {
	return g.name
}

func (g *Group) initPeers() {
	if g.peers == nil {
		g.peers = getPeers(g.name)
	}
}

// Get is a method to be called when getting a value from the cache locally.
// It is not intended to be called by peers over the network.
func (g *Group) Get(ctx context.Context, key string, dest Sink) error {
	// use get method with g.load as the load function which can load from both local and remote.
	return g.get(ctx, key, dest, g.load)
}

func (g *Group) Set(ctx context.Context, key string, value []byte, expire time.Time, hotCache bool) error {
	g.peersOnce.Do(g.initPeers)

	if key == "" {
		return errors.New("empty Set() key not allowed")
	}

	_, err := g.setGroup.Do(ctx, key, func() (interface{}, error) {
		executed, err := g.setIfRemoteOwner(ctx, key, value, expire, hotCache)
		if err != nil {
			return nil, err
		}
		if executed {
			return nil, nil
		}
		// we own this key so we populate the main cache
		g.localMainCacheSet(key, value, expire)
		// and we always populate the hot cache to maximize chances of early cache hits
		g.localHotCacheSet(key, value, expire)
		// and then let's call all other peers to clear the key from their caches
		err = g.removeFromPeers(ctx, key, nil)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

// Remove clears the key from our cache then forwards the remove
// request to all peers.
func (g *Group) Remove(ctx context.Context, key string) error {
	g.peersOnce.Do(g.initPeers)

	_, err := g.removeGroup.Do(ctx, key, func() (interface{}, error) {
		remoteOwner, remoteOwnerRemoveErr := g.removeFromRemoteOwner(ctx, key)

		// remove from our cache next
		g.localRemove(key)

		// then clear the key from all hot and main caches of peers
		peersRemoveErr := g.removeFromPeers(ctx, key, remoteOwner)

		return nil, errors.Join(remoteOwnerRemoveErr, peersRemoveErr)
	})
	return err
}

// getForRemote is a method to be called by peers over the network when getting a value from the cache.
func (g *Group) getForRemote(ctx context.Context, key string, dest Sink) error {
	// use get method with g.loadLocally as the load function which only loads locally without trying to get from peers to avoid infinite recursion between peers.
	return g.get(ctx, key, dest, g.loadLocally)
}

type loadFunc func(ctx context.Context, key string, dest Sink) (value ByteView, destPopulated bool, err error)

func (g *Group) get(ctx context.Context, key string, dest Sink, load loadFunc) error {
	g.peersOnce.Do(g.initPeers)
	g.Stats.Gets.Add(1)
	if dest == nil {
		return errors.New("groupcache: nil dest Sink")
	}

	value, cacheHit := g.lookupCaches(key)
	if cacheHit {
		g.Stats.CacheHits.Add(1)
		return setSinkView(dest, value)
	}

	// Optimization to avoid double unmarshalling or copying: keep
	// track of whether the dest was already populated. One caller
	// (if local) will set this; the losers will not. The common
	// case will likely be one caller.
	destPopulated := false
	value, destPopulated, err := load(ctx, key, dest)
	if err != nil {
		return err
	}
	if destPopulated {
		return nil
	}
	return setSinkView(dest, value)
}

// removeFromPeers removes the key from all peers that do not own it.
// This is used when a key is removed from the owner peer to ensure that stale values are not returned from other peers.
// It returns an error if any of the remove operations fail, but will attempt to remove the key from all peers regardless of individual errors.
func (g *Group) removeFromPeers(ctx context.Context, key string, owner ProtoGetter) error {
	errs := make(chan error)
	wg := sync.WaitGroup{}

	for _, peer := range g.peers.GetAll() {
		if peer == owner {
			continue
		}

		wg.Add(1)
		go func(peer ProtoGetter) {
			errs <- g.removeFromPeer(ctx, peer, key)
			wg.Done()
		}(peer)
	}
	go func() {
		wg.Wait()
		close(errs)
	}()

	var err error
	for e := range errs {
		err = errors.Join(err, e)
	}
	return err
}

// load loads key either by invoking the getter locally or by sending it to another machine.
func (g *Group) load(ctx context.Context, key string, dest Sink) (value ByteView, destPopulated bool, err error) {
	g.Stats.Loads.Add(1)
	viewi, err := g.loadGroup.Do(ctx, key, func() (interface{}, error) {
		g.Stats.LoadsDeduped.Add(1)
		var value ByteView
		var err error
		var loadedFromRemote bool
		loadedFromRemote, value, err = g.loadFromRemoteIfRemoteOwner(ctx, key)
		if loadedFromRemote && err == nil {
			return value, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, &ErrNotFound{}) {
			return nil, err
			// otherwise we want to attempt to load locally
		}

		value, destPopulated, err = g.loadLocally(ctx, key, dest)
		if err != nil {
			return nil, err
		}
		return value, nil
	})
	if err == nil {
		value = viewi.(ByteView)
	}
	return
}

// loadLocally loads key either by invoking the getter locally or by sending it to another machine.
func (g *Group) loadLocally(ctx context.Context, key string, dest Sink) (value ByteView, destPopulated bool, err error) {
	g.Stats.LocalLoads.Add(1)
	// Load locally:
	// - checks if the key is owned by this peer
	// - if owned, checks the main cache for the key
	// - if not owned or not found in the main cache, calls the getter locally
	// - if owned, populates the main cache with the value returned by the getter
	viewi, err := g.localLoadGroup.Do(ctx, key, func() (interface{}, error) {
		g.Stats.LocalLoadsDeduped.Add(1)
		_, notOwned := g.peers.PickPeer(key)

		if !notOwned { // == owned
			// since we own the key, let's lookup the main cache before calling the getter locally
			if value, cacheHit := g.lookupMainCache(key); cacheHit {
				g.Stats.CacheHits.Add(1)
				// this is cached locally at the peer that owns the key, let's populate the hot cache
				g.populateHotCache(key, value)
				return value, nil
			}
		}

		// we either do not own the key actually or we own the key but it is not in the main cache,
		// so we call the getter locally
		value, err = g.getLocally(ctx, key, dest)
		if err != nil {
			g.Stats.LocalLoadErrs.Add(1)
			return nil, err
		}
		g.Stats.SuccessfulLocalLoads.Add(1)
		destPopulated = true // only one caller of load gets this return value

		if notOwned {
			// if we don't own the key...
			if g.opts.clampNotOwnedKeyTTL != nil {
				// ...and clampNotOwnedKeyTTL options is set:
				// - we set the expire time to now + notOwnedKeyTTL to prevent values served by the peers that do not own the key from being cached for long.
				//   (unless the key has expiry time that is sooner than now + notOwnedKeyTTL, in which case we keep the expiry time returned
				//    by the getter to avoid caching stale values in the hot cache)
				notOwnedMaxExpiryTime := time.Now().Add(*g.opts.clampNotOwnedKeyTTL)
				if value.e.IsZero() || value.e.After(notOwnedMaxExpiryTime) {
					value.e = notOwnedMaxExpiryTime
				}
			}
		} else {
			// we own the key, so let's populate the main cache
			g.populateMainCache(key, value)
		}
		// always populate the hot cache
		g.populateHotCache(key, value)
		return value, nil
	})
	if err == nil {
		value = viewi.(ByteView)
	}
	return
}

func (g *Group) getLocally(ctx context.Context, key string, dest Sink) (ByteView, error) {
	err := g.getter.Get(ctx, key, dest)
	if err != nil {
		return ByteView{}, err
	}
	return dest.view()
}

func (g *Group) loadFromRemoteIfRemoteOwner(ctx context.Context, key string) (loaded bool, value ByteView, err error) {
	loaded, _, err = g.callRemoteIfRemoteOwner(ctx, key, func(ctx context.Context, peer ProtoGetter) error {
		// we do not own the key
		// let's check the hot cache again because it might got populated before we entered
		// singleflight
		var cacheHit bool
		if value, cacheHit = g.lookupHotCache(key); cacheHit {
			g.Stats.CacheHits.Add(1)
			return nil
		}

		// metrics duration start
		start := time.Now()

		// get value from peers
		value, err = g.getFromPeer(ctx, peer, key)

		// metrics duration compute
		duration := int64(time.Since(start)) / int64(time.Millisecond)

		// metrics only store the slowest duration
		if g.Stats.GetFromPeersLatencyLower.Get() < duration {
			g.Stats.GetFromPeersLatencyLower.Store(duration)
		}
		if err == nil {
			// populate hot cache with the value retrieved from peer
			g.populateHotCache(key, value)
			g.Stats.PeerLoads.Add(1)
			return nil
		}
		return err
	})
	return loaded, value, err
}

func (g *Group) setIfRemoteOwner(ctx context.Context, key string, value []byte, expire time.Time, hotCache bool) (bool, error) {
	remoteOwner, owner, err := g.callRemoteIfRemoteOwner(ctx, key, func(ctx context.Context, peer ProtoGetter) error {
		// we do not own the key, so we set the value remotely to the owner peer
		return g.setFromPeer(ctx, peer, key, value, expire)
	})
	if err != nil {
		return remoteOwner, err
	}
	if remoteOwner {
		// If hotCache is true, we set local hot cache.
		// TODO(thrawn01): Not sure if this is useful outside of tests...
		//  maybe we should ALWAYS update the local cache?
		if hotCache {
			g.localHotCacheSet(key, value, expire)
		}
		// and then we clear the key from all the peers that are not key owner to prevent stale values from being served from those peers.
		err := g.removeFromPeers(ctx, key, owner)
		if err != nil {
			return true, err
		}
		return true, nil
	}
	return false, nil
}

func (g *Group) removeFromRemoteOwner(ctx context.Context, key string) (ProtoGetter, error) {
	remoteOwner, owner, err := g.callRemoteIfRemoteOwner(ctx, key, func(ctx context.Context, peer ProtoGetter) error {
		// we do not own the key, so we remove the key remotely from the owner peer
		return g.removeFromPeer(ctx, peer, key)
	})
	if err != nil {
		return owner, err
	}
	if remoteOwner {
		return owner, nil
	}
	return nil, nil
}

// callRemoteIfRemoteOwner calls the given function if the key is owned by a remote peer. It retries the call if there is an remote call error while calling the remote peer
// until maxRemoteCallErrorRetryTime is reached.
// It returns a boolean indicating whether the owner is remote, the ProtoGetter of the remote owner if the owner is remote, and an error if any error occurs during the call.
func (g *Group) callRemoteIfRemoteOwner(ctx context.Context, key string, fn func(ctx context.Context, peer ProtoGetter) error) (bool, ProtoGetter, error) {
	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = remoteCallErrorBackoffInitial
	eb.MaxInterval = remoteCallErrorBackoffMaxInterval
	eb.MaxElapsedTime = maxRemoteCallErrorRetryTime
	boCtx := backoff.WithContext(eb, ctx)

	var remoteOwner bool
	var owner ProtoGetter

	err := backoff.Retry(func() error {
		var ok bool
		owner, ok = g.peers.PickPeer(key)
		if !ok {
			remoteOwner = false
			return nil
		}
		remoteOwner = true

		err := fn(ctx, owner)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, &ErrNotFound{}) {
				// we don't want to record or log them, this is not really an error
				return backoff.Permanent(err)
			}
			g.Stats.PeerErrors.Add(1)
			trace.SpanFromContext(ctx).RecordError(err, trace.WithAttributes(otelAttributePeerURL.String(owner.GetURL())))
			if errors.Is(err, &ErrRemoteCall{}) {
				logger.Info().WithFields(map[string]interface{}{
					"err":      err,
					"key":      key,
					"category": "groupcache",
				}).Printf("error calling peer '%s'", owner.GetURL())
				return err
			}
			logger.Error().WithFields(map[string]interface{}{
				"err":      err,
				"key":      key,
				"category": "groupcache",
			}).Printf("unexpected error calling peer '%s'", owner.GetURL())
			return backoff.Permanent(err)
		}

		return nil
	}, boCtx)
	return remoteOwner, owner, err
}

func (g *Group) getFromPeer(ctx context.Context, peer ProtoGetter, key string) (ByteView, error) {
	ctx, cancel := context.WithTimeout(ctx, remoteOperationTimeout)
	defer cancel()
	req := &pb.GetRequest{
		Group: &g.name,
		Key:   &key,
	}
	res := &pb.GetResponse{}
	err := peer.Get(ctx, req, res)
	if err != nil {
		return ByteView{}, err
	}

	var expire time.Time
	if res.Expire != nil && *res.Expire != 0 {
		expire = time.Unix(*res.Expire/int64(time.Second), *res.Expire%int64(time.Second))
		if time.Now().After(expire) {
			return ByteView{}, errors.New("peer returned expired value")
		}
	}

	value := ByteView{b: res.Value, e: expire}

	return value, nil
}

func (g *Group) setFromPeer(ctx context.Context, peer ProtoGetter, k string, v []byte, e time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, remoteOperationTimeout)
	defer cancel()

	var expire int64
	if !e.IsZero() {
		expire = e.UnixNano()
	}
	req := &pb.SetRequest{
		Expire: &expire,
		Group:  &g.name,
		Key:    &k,
		Value:  v,
	}
	return peer.Set(ctx, req)
}

func (g *Group) removeFromPeer(ctx context.Context, peer ProtoGetter, key string) error {
	ctx, cancel := context.WithTimeout(ctx, remoteOperationTimeout)
	defer cancel()
	req := &pb.GetRequest{
		Group: &g.name,
		Key:   &key,
	}
	return peer.Remove(ctx, req)
}

func (g *Group) lookupCaches(key string) (value ByteView, cacheHit bool) {
	//  first check hot cache, it's short-lived and if we have it, we can respond quickly
	value, cacheHit = g.lookupHotCache(key)
	if cacheHit {
		return value, true
	}

	// check if we own the key
	_, notOwned := g.peers.PickPeer(key)
	// if we do own the key, check the main cache
	if !notOwned { // == owned
		return g.lookupMainCache(key)
	}
	return value, false
}

func (g *Group) lookupMainCache(key string) (value ByteView, ok bool) {
	return g.lookupCache(key, &g.mainCache)
}

func (g *Group) lookupHotCache(key string) (value ByteView, ok bool) {
	if g.opts.disableHotCache {
		return ByteView{}, false
	}
	return g.lookupCache(key, &g.hotCache)
}

func (g *Group) lookupCache(key string, c *cache) (value ByteView, ok bool) {
	if g.cacheBytes <= 0 {
		return
	}
	value, ok = c.get(key)
	return
}

func (g *Group) localMainCacheSet(key string, value []byte, expire time.Time) {
	if g.cacheBytes <= 0 {
		return
	}
	g.peersOnce.Do(g.initPeers)

	bv := ByteView{
		b: value,
		e: expire,
	}

	// Ensure no loads in flight.
	// Main cache can only be populated by local loads, so we only need to acquire the local load lock to ensure no loads are in flight.
	g.localLoadGroup.Lock(func() {
		_, notOwned := g.peers.PickPeer(key)
		if notOwned {
			return
		}
		g.populateMainCache(key, bv)
	})
}

func (g *Group) localHotCacheSet(key string, value []byte, expire time.Time) {
	if g.opts.disableHotCache {
		return
	}
	if g.cacheBytes <= 0 {
		return
	}

	bv := ByteView{
		b: value,
		e: expire,
	}

	// Ensure no loads in flight.
	// Hot cache can be populated by both local local and remote loads, so we need to acquire both locks to ensure no loads are in flight.
	g.loadGroup.Lock(func() {
		g.localLoadGroup.Lock(func() {
			g.populateHotCache(key, bv)
		})
	})
}

func (g *Group) localRemove(key string) {
	// Clear key from our local cache
	if g.cacheBytes <= 0 {
		return
	}

	// Ensure no requests are in flight
	g.loadGroup.Lock(func() {
		g.localLoadGroup.Lock(func() {
			g.hotCache.remove(key)
			g.mainCache.remove(key)
		})
	})
}

func (g *Group) populateHotCache(key string, value ByteView) {
	if g.opts.disableHotCache {
		return
	}
	if g.cacheBytes <= 0 {
		return
	}
	defer g.evictFromCacheIfNeeded()

	if g.opts.clampHotCacheMaxTTL != nil {
		// if the clampHotCacheMaxTTL option is set, we set the expire time for the hot cache entry to be no later than now + clampHotCacheMaxTTL to prevent values
		// from being cached in the hot cache for too long which can cause staleness on certain pods.
		hotCacheMaxExpiryTime := time.Now().Add(*g.opts.clampHotCacheMaxTTL)
		if value.e.IsZero() || value.e.After(hotCacheMaxExpiryTime) {
			value.e = hotCacheMaxExpiryTime
		}
	}
	g.hotCache.add(key, value)
}

func (g *Group) populateMainCache(key string, value ByteView) {
	if g.cacheBytes <= 0 {
		return
	}
	defer g.evictFromCacheIfNeeded()
	g.mainCache.add(key, value)
}

func (g *Group) evictFromCacheIfNeeded() {
	for {
		mainBytes := g.mainCache.bytes()
		hotBytes := g.hotCache.bytes()
		if mainBytes+hotBytes <= g.cacheBytes {
			return
		}

		// TODO(bradfitz): this is good-enough-for-now logic.
		// It should be something based on measurements and/or
		// respecting the costs of different resources.
		victim := &g.mainCache
		if hotBytes > mainBytes/8 {
			victim = &g.hotCache
		}
		victim.removeOldest()
	}
}

// CacheType represents a type of cache.
type CacheType int

const (
	// The MainCache is the cache for items that this peer is the
	// owner for.
	MainCache CacheType = iota + 1

	// The HotCache is the cache for items that seem popular
	// enough to replicate to this node, even though it's not the
	// owner.
	HotCache
)

// CacheStats returns stats about the provided cache within the group.
func (g *Group) CacheStats(which CacheType) CacheStats {
	switch which {
	case MainCache:
		return g.mainCache.stats()
	case HotCache:
		return g.hotCache.stats()
	default:
		return CacheStats{}
	}
}

// NowFunc returns the current time which is used by the LRU to
// determine if the value has expired. This can be overridden by
// tests to ensure items are evicted when expired.
var NowFunc lru.NowFunc = time.Now

// cache is a wrapper around an *lru.Cache that adds synchronization,
// makes values always be ByteView, and counts the size of all keys and
// values.
type cache struct {
	mu         sync.RWMutex
	nbytes     int64 // of all keys and values
	lru        *lru.Cache
	nhit, nget int64
	nevict     int64 // number of evictions
}

func (c *cache) stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Bytes:     c.nbytes,
		Items:     c.itemsLocked(),
		Gets:      c.nget,
		Hits:      c.nhit,
		Evictions: c.nevict,
	}
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = &lru.Cache{
			Now: NowFunc,
			OnEvicted: func(key lru.Key, value interface{}) {
				val := value.(ByteView)
				c.nbytes -= int64(len(key.(string))) + int64(val.Len())
				c.nevict++
			},
		}
	}
	c.lru.Add(key, value, value.Expire())
	c.nbytes += int64(len(key)) + int64(value.Len())
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nget++
	if c.lru == nil {
		return
	}
	vi, ok := c.lru.Get(key)
	if !ok {
		return
	}
	c.nhit++
	return vi.(ByteView), true
}

func (c *cache) remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	c.lru.Remove(key)
}

func (c *cache) removeOldest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.RemoveOldest()
	}
}

func (c *cache) bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nbytes
}

func (c *cache) items() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.itemsLocked()
}

func (c *cache) itemsLocked() int64 {
	if c.lru == nil {
		return 0
	}
	return int64(c.lru.Len())
}

// An AtomicInt is an int64 to be accessed atomically.
type AtomicInt int64

// Add atomically adds n to i.
func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

// Store atomically stores n to i.
func (i *AtomicInt) Store(n int64) {
	atomic.StoreInt64((*int64)(i), n)
}

// Get atomically gets the value of i.
func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

// CacheStats are returned by stats accessors on Group.
type CacheStats struct {
	Bytes     int64
	Items     int64
	Gets      int64
	Hits      int64
	Evictions int64
}
