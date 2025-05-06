/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;
import java.util.Map;

/**
 * 描述在调用{@link Admin#createPartitions(Map)}时为特定主题创建新分区的配置类。
 * 该类用于以下场景：
 * 1. 当需要扩展主题的吞吐量时，通过增加分区数来实现水平扩展
 * 2. 在集群扩容后，需要重新平衡分区分配
 * 3. 当某些分区负载过重时，通过增加分区来分散负载
 * 
 * 该类的API仍在演进中，详情请参考{@link Admin}。
 */
@InterfaceStability.Evolving
public class NewPartitions {

    /**
     * 操作完成后的目标分区总数
     * 这个值必须大于当前主题的分区数
     */
    private final int totalCount;

    /**
     * 新分区的副本分配方案
     * - 外层List：每个元素代表一个新分区的副本分配
     * - 内层List<Integer>：表示单个分区的副本所在的broker ID列表
     * - 如果为null，则由控制器自动分配副本
     */
    private final List<List<Integer>> newAssignments;

    /**
     * 私有构造函数，用于创建NewPartitions实例
     * @param totalCount 目标分区总数
     * @param newAssignments 新分区的副本分配方案，可以为null
     */
    private NewPartitions(int totalCount, List<List<Integer>> newAssignments) {
        this.totalCount = totalCount;
        this.newAssignments = newAssignments;
    }

    /**
     * 增加主题的分区数到指定的总数，由broker自动决定新副本的分配方案
     * 使用场景：当不关心具体的副本分配方案，只需要增加分区数时使用此方法
     *
     * @param totalCount 操作完成后的目标分区总数，必须大于当前分区数
     * @return 返回NewPartitions实例，其中newAssignments为null，表示使用自动分配
     */
    public static NewPartitions increaseTo(int totalCount) {
        return new NewPartitions(totalCount, null);
    }

    /**
     * 增加主题的分区数到指定的总数，并指定新分区的副本分配方案
     * 使用场景：当需要精确控制新分区的副本分配时使用此方法
     * 
     * <p>注意事项：
     * 1. newAssignments的长度必须等于新增的分区数(totalCount - oldCount)
     * 2. 每个内层列表的长度必须等于主题的副本因子
     * 3. 每个内层列表的第一个broker ID将成为该分区的首选副本（preferred replica）
     * 4. 现有分区的分配方案不会改变</p>
     *
     * <p>示例：假设一个主题当前有3个分区，副本因子为2，
     * 现在要将分区数增加到6个，可以这样构造：</p>
     *
     * <pre><code>
     * NewPartitions.increaseTo(6, asList(asList(1, 2),  // 新分区3的副本分配
     *                                    asList(2, 3),  // 新分区4的副本分配
     *                                    asList(3, 1))) // 新分区5的副本分配
     * </code></pre>
     * <p>在这个例子中：
     * - 分区3的首选副本是broker 1，备份副本在broker 2
     * - 分区4的首选副本是broker 2，备份副本在broker 3
     * - 分区5的首选副本是broker 3，备份副本在broker 1</p>
     *
     * @param totalCount 操作完成后的目标分区总数
     * @param newAssignments 新分区的副本分配方案，每个内层列表指定一个新分区的副本分配
     * @return 返回配置好的NewPartitions实例
     */
    public static NewPartitions increaseTo(int totalCount, List<List<Integer>> newAssignments) {
        return new NewPartitions(totalCount, newAssignments);
    }

    /**
     * 获取操作完成后的目标分区总数
     * @return 返回目标分区总数
     */
    public int totalCount() {
        return totalCount;
    }

    /**
     * 获取新分区的副本分配方案
     * @return 如果指定了手动分配方案，返回分配方案列表；如果使用自动分配，返回null
     */
    public List<List<Integer>> assignments() {
        return newAssignments;
    }

    @Override
    public String toString() {
        return "(totalCount=" + totalCount() + ", newAssignments=" + assignments() + ")";
    }

}
