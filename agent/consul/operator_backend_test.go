// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package consul

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/consul/acl"
	external "github.com/hashicorp/consul/agent/grpc-external"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/proto/private/pboperator"
	"github.com/hashicorp/consul/sdk/testutil/retry"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/stretchr/testify/require"
	gogrpc "google.golang.org/grpc"

	"github.com/hashicorp/consul/sdk/testutil"
	"github.com/hashicorp/consul/testrpc"
)

func TestOperatorBackend_TransferLeader(t *testing.T) {
	t.Parallel()

	// Raft's LeadershipTransfer future succeeds when the leader leaves the
	// leader loop, including when it steps down because its lease expired
	// ("lost leadership during transfer") rather than because the chosen
	// follower won. A follow-up election can then re-elect the same server.
	// The previous check slept for a full lease (1s) and read one node's
	// cached LeaderWithID, which is cleared on step-down, so it often ran
	// after leadership had bounced back. Read raft state directly, inside a
	// window shorter than HeartbeatTimeout, and retry the call when this
	// attempt did not actually move leadership.
	conf := testClusterConfig{
		Datacenter: "dc1",
		Servers:    3,
		ServerConf: func(config *Config) {
			config.RaftConfig.HeartbeatTimeout = 2 * time.Second
			config.RaftConfig.ElectionTimeout = 2 * time.Second
			config.RaftConfig.LeaderLeaseTimeout = 1 * time.Second
		},
	}

	nodes := newTestCluster(t, &conf)
	s1 := nodes.Servers[0]
	// Make sure a leader is elected
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	// Dial one server. TransferLeader is forwarded to the current leader.
	// The context has to cover several transfer attempts: each attempt can
	// block until ElectionTimeout while raft waits for the leader to step down.
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	t.Cleanup(cancel)

	conn, err := gogrpc.DialContext(ctx, s1.config.RPCAddr.String(),
		gogrpc.WithContextDialer(newServerDialer(s1.config.RPCAddr.String())),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
		gogrpc.WithBlock())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	operatorClient := pboperator.NewOperatorServiceClient(conn)

	// singleLeader is the node ID of the only server in raft leader state.
	// An empty result means an election is in progress or more than one
	// server still reports itself as leader.
	singleLeader := func() string {
		var id string
		for _, srv := range nodes.Servers {
			if !srv.IsLeader() {
				continue
			}
			if id != "" {
				return ""
			}
			id = string(srv.config.NodeID)
		}
		return id
	}

	testutil.RunStep(t, "transfer leader", func(t *testing.T) {
		retry.RunWith(&retry.Timer{Wait: 200 * time.Millisecond, Timeout: 30 * time.Second}, t, func(r *retry.R) {
			before := singleLeader()
			if before == "" {
				r.Fatal("cluster has no single leader")
			}

			req := pboperator.TransferLeaderRequest{
				ID: "",
			}
			reply, err := operatorClient.TransferLeader(ctx, &req)
			if err != nil {
				r.Fatalf("TransferLeader: %v", err)
			}
			if reply == nil || !reply.Success {
				r.Fatal("TransferLeader returned success=false")
			}

			// The new leader is already elected when a real transfer returns.
			// Keep this shorter than HeartbeatTimeout (2s) so a later election
			// cannot hide that result.
			deadline := time.Now().Add(500 * time.Millisecond)
			for {
				after := singleLeader()
				if after != "" && after != before {
					return
				}
				if !time.Now().Before(deadline) {
					r.Fatalf("leader still %s", before)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
		testrpc.WaitForLeader(t, s1.RPC, "dc1")
	})
}

func TestOperatorBackend_TransferLeaderWithACL(t *testing.T) {
	t.Parallel()

	conf := testClusterConfig{
		Datacenter: "dc1",
		Servers:    3,
		ServerConf: func(config *Config) {
			config.RaftConfig.HeartbeatTimeout = 2 * time.Second
			config.RaftConfig.ElectionTimeout = 2 * time.Second
			config.RaftConfig.LeaderLeaseTimeout = 1 * time.Second
			config.ACLsEnabled = true
			config.ACLInitialManagementToken = "root"
			config.ACLResolverSettings.ACLDefaultPolicy = "deny"
		},
	}

	nodes := newTestCluster(t, &conf)
	s1 := nodes.Servers[0]
	// Make sure a leader is elected
	testrpc.WaitForLeader(t, s1.RPC, "dc1")
	// Make a write call to server2 and make sure it gets forwarded to server1
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	// Dial server2 directly
	conn, err := gogrpc.DialContext(ctx, s1.config.RPCAddr.String(),
		gogrpc.WithContextDialer(newServerDialer(s1.config.RPCAddr.String())),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
		gogrpc.WithBlock())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	operatorClient := pboperator.NewOperatorServiceClient(conn)

	codec := rpcClient(t, s1)
	rules := `operator = "write"`
	tokenWrite := createTokenWithPolicyNameFull(t, codec, "the-policy-write", rules, "root")
	rules = `operator = "read"`
	tokenRead := createToken(t, codec, rules)
	require.NoError(t, err)

	testutil.RunStep(t, "transfer leader no token", func(t *testing.T) {
		beforeLeader, _ := s1.raft.LeaderWithID()
		require.NotEmpty(t, beforeLeader)
		// Do the grpc Write call to server2
		req := pboperator.TransferLeaderRequest{
			ID: "",
		}
		reply, err := operatorClient.TransferLeader(ctx, &req)
		require.True(t, acl.IsErrPermissionDenied(err))
		require.Nil(t, reply)
		time.Sleep(1 * time.Second)
		testrpc.WaitForLeader(t, s1.RPC, "dc1")
		retry.RunWith(&retry.Timer{Wait: time.Second, Timeout: 3 * time.Second}, t, func(r *retry.R) {
			afterLeader, _ := s1.raft.LeaderWithID()
			require.NotEmpty(r, afterLeader)
			if afterLeader != beforeLeader {
				r.Fatalf("leader should have changed %s == %s", afterLeader, beforeLeader)
			}
		})

	})

	testutil.RunStep(t, "transfer leader operator read token", func(t *testing.T) {

		beforeLeader, _ := s1.raft.LeaderWithID()
		require.NotEmpty(t, beforeLeader)
		// Do the grpc Write call to server2
		req := pboperator.TransferLeaderRequest{
			ID: "",
		}

		ctxToken, err := external.ContextWithQueryOptions(ctx, structs.QueryOptions{Token: tokenRead})
		require.NoError(t, err)

		reply, err := operatorClient.TransferLeader(ctxToken, &req)
		require.True(t, acl.IsErrPermissionDenied(err))
		require.Nil(t, reply)
		testrpc.WaitForLeader(t, s1.RPC, "dc1")
		retry.RunWith(&retry.Timer{Wait: time.Second, Timeout: 3 * time.Second}, t, func(r *retry.R) {
			afterLeader, _ := s1.raft.LeaderWithID()
			require.NotEmpty(r, afterLeader)
			if afterLeader != beforeLeader {
				r.Fatalf("leader should have changed %s == %s", afterLeader, beforeLeader)
			}
		})
	})

	testutil.RunStep(t, "transfer leader operator write token", func(t *testing.T) {

		beforeLeader, _ := s1.raft.LeaderWithID()
		require.NotEmpty(t, beforeLeader)
		// Do the grpc Write call to server2
		req := pboperator.TransferLeaderRequest{
			ID: "",
		}
		ctxToken, err := external.ContextWithQueryOptions(ctx, structs.QueryOptions{Token: tokenWrite.SecretID})
		require.NoError(t, err)
		reply, err := operatorClient.TransferLeader(ctxToken, &req)
		require.NoError(t, err)
		require.True(t, reply.Success)
		time.Sleep(1 * time.Second)
		testrpc.WaitForLeader(t, s1.RPC, "dc1")
		retry.RunWith(&retry.Timer{Wait: 2 * time.Second, Timeout: 6 * time.Second}, t, func(r *retry.R) {
			afterLeader, _ := s1.raft.LeaderWithID()
			require.NotEmpty(r, afterLeader)
			if afterLeader == beforeLeader {
				r.Fatalf("leader should have changed %s == %s", afterLeader, beforeLeader)
			}
		})
	})
}
