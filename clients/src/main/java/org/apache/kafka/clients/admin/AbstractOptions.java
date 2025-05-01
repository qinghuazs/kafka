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


/**
 * 这个类实现了AdminClient命令的各种Options类共享的通用API
 * 
 * 作为所有管理客户端操作选项的基类，提供了超时时间等基础配置项。
 * 通过泛型参数T确保子类可以实现链式调用方法。
 */
public abstract class AbstractOptions<T extends AbstractOptions> {

    /**
     * 操作的超时时间（毫秒）
     * 如果为null，则使用AdminClient的默认API超时时间
     */
    protected Integer timeoutMs = null;

    /**
     * 设置此操作的超时时间（毫秒）
     * 如果设置为null，将使用AdminClient的默认API超时时间
     *
     * @param timeoutMs 超时时间，单位为毫秒
     * @return 返回当前对象实例，支持方法链式调用
     */
    @SuppressWarnings("unchecked")
    public T timeoutMs(Integer timeoutMs) {
        // 设置超时时间字段
        this.timeoutMs = timeoutMs;
        // 返回当前实例，支持链式调用
        return (T) this;
    }

    /**
     * 获取此操作的超时时间（毫秒）
     * 如果返回null，表示将使用AdminClient的默认API超时时间
     *
     * @return 超时时间，单位为毫秒
     */
    public Integer timeoutMs() {
        return timeoutMs;
    }

}
