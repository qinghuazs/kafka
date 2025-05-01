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
 * 用于配置中止事务操作的选项类。
 * 
 * <p>该类继承自AbstractOptions，用于在Kafka Admin客户端执行中止事务操作时，
 * 提供额外的配置选项。目前主要支持超时时间的设置，该设置继承自AbstractOptions类。
 * 
 * <p>应用场景：
 * 1. 在分布式事务处理中，当需要回滚或取消一个正在进行的事务时使用
 * 2. 在发生错误或需要取消事务操作时，用于设置中止事务的相关参数
 * 3. 可以通过timeoutMs()方法设置操作超时时间，避免事务中止操作长时间阻塞
 * 
 * <p>该类的API仍在演进中，后续版本可能会增加更多的配置选项
 */
@InterfaceStability.Evolving
public class AbortTransactionOptions extends AbstractOptions<AbortTransactionOptions> {

    /**
     * 重写toString方法，用于返回当前选项实例的字符串表示
     * 
     * @return 返回包含超时时间配置的字符串表示
     */
    @Override
    public String toString() {
        return "AbortTransactionOptions(" +
            "timeoutMs=" + timeoutMs +
            ')';
    }

}
