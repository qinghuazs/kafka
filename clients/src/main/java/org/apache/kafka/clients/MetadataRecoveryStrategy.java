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
package org.apache.kafka.clients;

import java.util.Locale;

/**
 * 定义了Kafka客户端在所有已知节点都不可用时可以采取的恢复策略。
 * 当Kafka客户端无法连接到任何已知的broker节点时，可以选择以下两种策略之一来恢复元数据：
 * 1. NONE - 不执行任何恢复操作，继续使用现有的元数据
 * 2. REBOOTSTRAP - 重新引导，尝试重新发现和连接集群中的可用节点
 */
public enum MetadataRecoveryStrategy {
    // 不执行任何恢复操作，保持当前状态
    NONE("none"),
    // 重新引导策略，尝试重新连接和发现集群节点
    REBOOTSTRAP("rebootstrap");

    // 策略的名称，用于在配置中标识具体的恢复策略
    public final String name;

    // 构造函数，初始化策略名称
    MetadataRecoveryStrategy(String name) {
        this.name = name;
    }

    /**
     * 根据策略名称返回对应的MetadataRecoveryStrategy枚举实例
     * 
     * @param name 策略名称字符串
     * @return 对应的MetadataRecoveryStrategy枚举实例
     * @throws IllegalArgumentException 当策略名称为null或不存在时抛出异常
     */
    public static MetadataRecoveryStrategy forName(String name) {
        // 检查输入参数是否为null
        if (name == null) {
            throw new IllegalArgumentException("Illegal MetadataRecoveryStrategy: null");
        }
        try {
            // 将输入的策略名称转换为大写并查找对应的枚举值
            return MetadataRecoveryStrategy.valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            // 如果找不到对应的策略，抛出IllegalArgumentException异常
            throw new IllegalArgumentException("Illegal MetadataRecoveryStrategy: " + name);
        }
    }
}
