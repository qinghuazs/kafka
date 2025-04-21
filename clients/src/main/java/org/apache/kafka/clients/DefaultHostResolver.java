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

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Kafka的默认主机名解析器实现类，用于将主机名解析为对应的IP地址数组。
 * 该类实现了HostResolver接口，提供了基础的DNS解析功能。
 */
public class DefaultHostResolver implements HostResolver {

    /**
     * 将指定的主机名解析为对应的IP地址数组
     * 
     * @param host 需要解析的主机名字符串
     * @return 返回与该主机名关联的所有IP地址数组
     * @throws UnknownHostException 当无法解析主机名时抛出此异常
     */
    @Override
    public InetAddress[] resolve(String host) throws UnknownHostException {
        // 调用Java标准库的InetAddress.getAllByName方法执行实际的DNS解析
        // 该方法会返回与主机名关联的所有IP地址（支持多IP的情况）
        return InetAddress.getAllByName(host);
    }
}
