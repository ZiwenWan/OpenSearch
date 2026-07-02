/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;

public class IndexGroupBalanceTests extends OpenSearchAllocationTestCase {

    private final Logger logger = LogManager.getLogger(IndexGroupBalanceTests.class);

    /**
     * Test that grouped indices have their shards balanced together across nodes.
     *
     * Setup: 6 nodes, 4 indices (5 primary shards, 1 replica each).
     *   - index-a1 and index-a2 in group "group-a"
     *   - index-b1 and index-b2 in group "group-b"
     *
     * Each group has 2 * 5 * 2 = 20 total shards.
     * With 6 nodes, avg shards per group per node = 20/6 ≈ 3.33.
     * No node should have significantly more than ceil(3.33) = 4 shards from one group.
     */
    public void testGroupBalanceDistributesGroupShardsEvenly() {
        AllocationService strategy = createAllocationService(getSettings().build(), new TestGatewayAllocator());

        int numberOfNodes = 6;
        int numberOfShards = 5;
        int numberOfReplicas = 1;

        ClusterState clusterState = initClusterWithGroups(strategy, numberOfNodes, numberOfShards, numberOfReplicas);

        Map<String, Map<String, Integer>> groupShardsPerNode = countGroupShardsPerNode(clusterState);

        int totalGroupShards = numberOfShards * (1 + numberOfReplicas) * 2; // 2 indices per group
        float avgGroupShardsPerNode = (float) totalGroupShards / numberOfNodes;
        int maxAllowed = (int) Math.ceil(avgGroupShardsPerNode) + 1;

        for (RoutingNode node : clusterState.getRoutingNodes()) {
            for (String group : new String[] { "group-a", "group-b" }) {
                int count = groupShardsPerNode.getOrDefault(node.nodeId(), new HashMap<>()).getOrDefault(group, 0);
                assertTrue(
                    "Node " + node.nodeId() + " has " + count + " shards from " + group + " (max allowed: " + maxAllowed + ")",
                    count <= maxAllowed
                );
            }
        }
    }

    /**
     * Test that ungrouped indices are still balanced per-index as before.
     */
    public void testUngroupedIndicesUnaffected() {
        AllocationService strategy = createAllocationService(getSettings().build(), new TestGatewayAllocator());

        int numberOfNodes = 4;
        int numberOfShards = 3;
        int numberOfReplicas = 1;

        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        // Grouped indices
        for (String indexName : new String[] { "grouped-1", "grouped-2" }) {
            IndexMetadata.Builder index = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(settings(Version.CURRENT).build())
                        .put("index.routing.allocation.index_group", "my-group")
                )
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas);
            metadataBuilder.put(index);
        }

        // Ungrouped indices
        for (String indexName : new String[] { "ungrouped-1", "ungrouped-2" }) {
            IndexMetadata.Builder index = IndexMetadata.builder(indexName)
                .settings(settings(Version.CURRENT).build())
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas);
            metadataBuilder.put(index);
        }

        Metadata metadata = metadataBuilder.build();
        for (IndexMetadata cursor : metadata.indices().values()) {
            routingTableBuilder.addAsNew(cursor);
        }

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(newNode("node" + i));
        }

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        clusterState = applyAllocationUntilNoChange(clusterState, strategy);

        // All shards should be assigned
        assertEquals(0, clusterState.getRoutingNodes().unassigned().size());

        // Ungrouped indices should be balanced per-index
        for (String indexName : new String[] { "ungrouped-1", "ungrouped-2" }) {
            int totalShards = numberOfShards * (1 + numberOfReplicas);
            float avgPerNode = (float) totalShards / numberOfNodes;
            for (RoutingNode node : clusterState.getRoutingNodes()) {
                int count = 0;
                for (ShardRouting shard : node) {
                    if (shard.getIndexName().equals(indexName) && shard.state() == STARTED) {
                        count++;
                    }
                }
                assertTrue(
                    "Node " + node.nodeId() + " has " + count + " shards of " + indexName,
                    count <= Math.ceil(avgPerNode) + 1
                );
            }
        }
    }

    /**
     * Test that indices without index_group behave exactly as original implementation.
     */
    public void testNoGroupSameAsOriginal() {
        AllocationService strategy = createAllocationService(getSettings().build(), new TestGatewayAllocator());

        int numberOfNodes = 4;
        int numberOfShards = 4;
        int numberOfReplicas = 1;

        // Create indices without any group
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        for (int i = 0; i < 3; i++) {
            IndexMetadata.Builder index = IndexMetadata.builder("test" + i)
                .settings(settings(Version.CURRENT).build())
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas);
            metadataBuilder.put(index);
        }

        Metadata metadata = metadataBuilder.build();
        for (IndexMetadata cursor : metadata.indices().values()) {
            routingTableBuilder.addAsNew(cursor);
        }

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(newNode("node" + i));
        }

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        clusterState = applyAllocationUntilNoChange(clusterState, strategy);

        assertEquals(0, clusterState.getRoutingNodes().unassigned().size());

        // Each index has 4 * 2 = 8 shards across 4 nodes → avg 2 per node
        for (int i = 0; i < 3; i++) {
            String indexName = "test" + i;
            for (RoutingNode node : clusterState.getRoutingNodes()) {
                int count = 0;
                for (ShardRouting shard : node) {
                    if (shard.getIndexName().equals(indexName) && shard.state() == STARTED) {
                        count++;
                    }
                }
                assertTrue(
                    "Node " + node.nodeId() + " has " + count + " shards of " + indexName + " (expected ~2)",
                    count <= 3
                );
            }
        }
    }

    /**
     * Simulate index rotation scenario: 2 rotation cycles, each producing 2 indices.
     * The latest cycle's indices should have their shards balanced together.
     */
    public void testIndexRotationScenario() {
        AllocationService strategy = createAllocationService(getSettings().build(), new TestGatewayAllocator());

        int numberOfNodes = 8;
        int numberOfShards = 4;
        int numberOfReplicas = 1;

        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        // Old rotation cycle (read-only)
        String[] oldCycleIndices = { "m3-regular-2024-06-19-00", "m3-large-2024-06-19-00" };
        for (String indexName : oldCycleIndices) {
            IndexMetadata.Builder index = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(settings(Version.CURRENT).build())
                        .put("index.routing.allocation.index_group", "cycle-2024-06-19-00")
                )
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas);
            metadataBuilder.put(index);
        }

        // Current rotation cycle (write-active)
        String[] newCycleIndices = { "m3-regular-2024-06-19-02", "m3-large-2024-06-19-02" };
        for (String indexName : newCycleIndices) {
            IndexMetadata.Builder index = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(settings(Version.CURRENT).build())
                        .put("index.routing.allocation.index_group", "cycle-2024-06-19-02")
                )
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas);
            metadataBuilder.put(index);
        }

        Metadata metadata = metadataBuilder.build();
        for (IndexMetadata cursor : metadata.indices().values()) {
            routingTableBuilder.addAsNew(cursor);
        }

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(newNode("node" + i));
        }

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        clusterState = applyAllocationUntilNoChange(clusterState, strategy);

        assertEquals(0, clusterState.getRoutingNodes().unassigned().size());

        // Verify each cycle's shards are spread evenly
        Map<String, Map<String, Integer>> groupShardsPerNode = countGroupShardsPerNode(clusterState);

        int totalGroupShards = numberOfShards * (1 + numberOfReplicas) * 2; // 2 indices per cycle
        float avgGroupShardsPerNode = (float) totalGroupShards / numberOfNodes;
        int maxAllowed = (int) Math.ceil(avgGroupShardsPerNode) + 1;

        for (RoutingNode node : clusterState.getRoutingNodes()) {
            for (String cycle : new String[] { "cycle-2024-06-19-00", "cycle-2024-06-19-02" }) {
                int count = groupShardsPerNode.getOrDefault(node.nodeId(), new HashMap<>()).getOrDefault(cycle, 0);
                assertTrue(
                    "Node " + node.nodeId() + " has " + count + " shards from " + cycle + " (max allowed: " + maxAllowed + ")",
                    count <= maxAllowed
                );
            }
        }
    }

    private Settings.Builder getSettings() {
        Settings.Builder settings = Settings.builder();
        settings.put(
            ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
        );
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.55f);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.45f);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 1.0f);
        return settings;
    }

    private ClusterState initClusterWithGroups(
        AllocationService strategy,
        int numberOfNodes,
        int numberOfShards,
        int numberOfReplicas
    ) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        String[][] groupedIndices = {
            { "index-a1", "group-a" },
            { "index-a2", "group-a" },
            { "index-b1", "group-b" },
            { "index-b2", "group-b" } };

        for (String[] entry : groupedIndices) {
            String indexName = entry[0];
            String group = entry[1];
            IndexMetadata.Builder index = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(settings(Version.CURRENT).build())
                        .put("index.routing.allocation.index_group", group)
                )
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas);
            metadataBuilder.put(index);
        }

        Metadata metadata = metadataBuilder.build();
        for (IndexMetadata cursor : metadata.indices().values()) {
            routingTableBuilder.addAsNew(cursor);
        }

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(newNode("node" + i));
        }

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        return applyAllocationUntilNoChange(clusterState, strategy);
    }

    private ClusterState applyAllocationUntilNoChange(ClusterState clusterState, AllocationService strategy) {
        clusterState = strategy.reroute(clusterState, "reroute");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = strategy.reroute(clusterState, "reroute");
        return applyStartedShardsUntilNoChange(clusterState, strategy);
    }

    private Map<String, Map<String, Integer>> countGroupShardsPerNode(ClusterState clusterState) {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        Metadata metadata = clusterState.metadata();

        for (RoutingNode node : clusterState.getRoutingNodes()) {
            Map<String, Integer> groupCounts = new HashMap<>();
            for (ShardRouting shard : node) {
                if (shard.state() != STARTED) continue;
                String group = IndexMetadata.INDEX_ROUTING_ALLOCATION_INDEX_GROUP_SETTING.get(
                    metadata.index(shard.getIndexName()).getSettings()
                );
                if (group != null && !group.isEmpty()) {
                    groupCounts.merge(group, 1, Integer::sum);
                }
            }
            result.put(node.nodeId(), groupCounts);
        }
        return result;
    }
}
