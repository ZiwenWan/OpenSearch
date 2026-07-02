/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import java.util.function.Predicate;

/**
 * Defines different constraints definitions
 *
 * @opensearch.internal
 */
public class ConstraintTypes {
    public final static long CONSTRAINT_WEIGHT = 1000000L;

    /**
     * Defines per index constraint which is breached when a node contains more than avg number of primary shards for an index
     */
    public final static String INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID = "index.primary.shard.balance.constraint";

    /**
     * Defines a cluster constraint which is breached when a node contains more than avg primary shards across all indices
     */
    public final static String CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID = "cluster.primary.shard.balance.constraint";

    /**
     * Defines a cluster constraint which is breached when a node contains more than avg primary shards across all indices
     */
    public final static String CLUSTER_PRIMARY_SHARD_REBALANCE_CONSTRAINT_ID = "cluster.primary.shard.rebalance.constraint";

    /**
     * Defines an index constraint which is breached when a node contains more than avg number of shards for an index
     */
    public final static String INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID = "index.shard.count.constraint";

    /**
     * Constraint to control number of shards of an index allocated on a single
     * node.
     * <p>
     * When an index belongs to a group, this constraint uses group-level shard
     * counts instead of per-index counts, treating all indices in the group as
     * one unit.
     */
    public static Predicate<Constraint.ConstraintParams> isIndexShardsPerNodeBreached() {
        return (params) -> {
            String group = params.getNode().getGroupForIndex(params.getIndex());
            if (group != null) {
                int currGroupShardsOnNode = params.getNode().numGroupShards(group);
                int allowedGroupShardsPerNode = (int) Math.ceil(params.getBalancer().avgShardsPerGroup(group));
                return currGroupShardsOnNode >= allowedGroupShardsPerNode;
            }
            int currIndexShardsOnNode = params.getNode().numShards(params.getIndex());
            int allowedIndexShardsPerNode = (int) Math.ceil(params.getBalancer().avgShardsPerNode(params.getIndex()));
            return (currIndexShardsOnNode >= allowedIndexShardsPerNode);
        };
    }

    /**
     * Defines a predicate which returns true when specific to an index (or its group),
     * a node contains more than average number of primary shards. When an index belongs
     * to a group, this uses group-level primary shard counts.
     */
    public static Predicate<Constraint.ConstraintParams> isPerIndexPrimaryShardsPerNodeBreached() {
        return (params) -> {
            String group = params.getNode().getGroupForIndex(params.getIndex());
            if (group != null) {
                int groupPrimaryShardCount = params.getNode().numGroupPrimaryShards(group);
                int allowedCount = (int) Math.ceil(params.getBalancer().avgPrimaryShardsPerGroup(group));
                return groupPrimaryShardCount >= allowedCount;
            }
            int perIndexPrimaryShardCount = params.getNode().numPrimaryShards(params.getIndex());
            int perIndexAllowedPrimaryShardCount = (int) Math.ceil(params.getBalancer().avgPrimaryShardsPerNode(params.getIndex()));
            return perIndexPrimaryShardCount >= perIndexAllowedPrimaryShardCount;
        };
    }

    /**
     * Defines a predicate which returns true when a node contains more than average number of primary shards with added buffer. This
     * constraint is used in weight calculation during allocation/rebalance both. When breached a high weight {@link ConstraintTypes#CONSTRAINT_WEIGHT}
     * is assigned to node resulting in lesser chances of node being selected as allocation/rebalance target
     */
    public static Predicate<Constraint.ConstraintParams> isPrimaryShardsPerNodeBreached(float buffer) {
        return (params) -> {
            int primaryShardCount = params.getNode().numPrimaryShards();
            int allowedPrimaryShardCount = (int) Math.ceil(params.getBalancer().avgPrimaryShardsPerNode() * (1 + buffer));
            return primaryShardCount >= allowedPrimaryShardCount;
        };
    }

    public static long predicateKeyToWeightMap(String key, long primaryConstraintWeight) {
        switch (key) {
            case CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID:
            case CLUSTER_PRIMARY_SHARD_REBALANCE_CONSTRAINT_ID:
                return primaryConstraintWeight;
            default:
                return CONSTRAINT_WEIGHT;
        }
    }
}
