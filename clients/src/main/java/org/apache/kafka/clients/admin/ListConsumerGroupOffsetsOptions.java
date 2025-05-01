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


/**
 * 用于Admin#listConsumerGroupOffsets(java.util.Map)和Admin#listConsumerGroupOffsets(String)方法的选项类。
 * 该类用于配置获取消费者组偏移量时的参数和选项。
 * <p>
 * 该类的API仍在演进中，详细信息请参见Admin接口的说明。
 */
@InterfaceStability.Evolving
public class ListConsumerGroupOffsetsOptions extends AbstractOptions<ListConsumerGroupOffsetsOptions> {

    /**
     * 是否要求返回稳定的偏移量结果
     * 当设置为true时，表示只返回已经提交且稳定的偏移量
     * 默认为false，表示返回所有可用的偏移量
     */
    private boolean requireStable = false;

    /**
     * 设置是否要求返回稳定的偏移量结果
     * 
     * @param requireStable 如果为true，则只返回稳定的偏移量；如果为false，返回所有偏移量
     * @return 返回当前对象以支持方法链式调用
     */
    public ListConsumerGroupOffsetsOptions requireStable(final boolean requireStable) {
        // 设置requireStable字段的值
        this.requireStable = requireStable;
        // 返回this以支持方法链式调用
        return this;
    }

    /**
     * 获取是否要求返回稳定的偏移量结果的设置
     * 
     * @return 如果要求返回稳定的偏移量则返回true，否则返回false
     */
    public boolean requireStable() {
        // 返回requireStable字段的值
        return requireStable;
    }
}
