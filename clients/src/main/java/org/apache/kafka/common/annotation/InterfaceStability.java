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
package org.apache.kafka.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * 接口稳定性注解
 * 用于告知用户某个包、类或方法在不同版本之间的变化程度。
 * 当前支持的稳定性级别有：{@link Stable}、{@link Evolving}和{@link Unstable}。
 * 
 * 应用场景：
 * 1. API版本管理
 * 2. 向后兼容性保证
 * 3. 接口稳定性声明
 * 4. 废弃策略管理
 */
@InterfaceStability.Evolving
public class InterfaceStability {
    /**
     * 稳定（Stable）接口注解
     * 
     * 兼容性保证：
     * - 在主版本、次版本和补丁版本中保持兼容性
     * - 例外情况：对于已经废弃至少一个主要/次要版本周期的API，
     *   可能在主版本发布(即0.m)时破坏兼容性
     * - 当破坏兼容性的影响较大时，需要至少一年的废弃期
     * 
     * 使用场景：
     * - 作为未标注的公共API的默认稳定性级别
     * - 用于标注长期稳定的核心功能接口
     * - 用于需要严格版本控制的公共API
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Stable { }

    /**
     * 演进中（Evolving）接口注解
     * 
     * 兼容性保证：
     * - 可能在次版本发布(即m.x)时破坏兼容性
     * 
     * 使用场景：
     * - 用于标注正在开发和改进中的功能
     * - 用于可能需要调整设计的新特性
     * - 用于实验性但已经相对稳定的功能
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Evolving { }

    /**
     * 不稳定（Unstable）接口注解
     * 
     * 兼容性保证：
     * - 不提供任何级别的发布粒度的可靠性或稳定性保证
     * 
     * 使用场景：
     * - 用于实验性功能
     * - 用于原型验证阶段的API
     * - 用于可能随时更改的内部接口
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Unstable { }
}
