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

package org.apache.kafka.common.network;

import java.net.Socket;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ServerConnectionId用于在服务器端唯一标识一个客户端连接。
 * 连接ID的格式为："localHost:localPort-remoteHost:remotePort-processorId-index"
 * 其中：
 * - localHost:localPort 表示服务器端本地监听地址和端口
 * - remoteHost:remotePort 表示远程客户端的地址和端口
 * - processorId 是处理该连接的处理器ID
 * - index 是用于确保连接ID唯一性的索引值
 * 
 * 应用场景：
 * 1. 在Kafka网络层中用于跟踪和管理TCP连接
 * 2. 支持IPv4和IPv6地址格式
 * 3. 用于连接的监控、调试和日志记录
 */
public class ServerConnectionId {

    // 用于解析host:port字符串的正则表达式，支持IPv4和IPv6地址格式
    // 注意：IPv6地址不应该包含在方括号中
    private static final Pattern HOST_PORT_PARSE_EXP = Pattern.compile("([0-9a-zA-Z\\-%._:]*):([0-9]+)");

    // 服务器端本地监听地址
    private final String localHost;
    // 服务器端本地监听端口
    private final int localPort;
    // 远程客户端地址
    private final String remoteHost;
    // 远程客户端端口
    private final int remotePort;
    // 处理该连接的处理器ID
    private final int processorId;
    // 确保连接ID唯一性的索引值
    private final int index;

    public ServerConnectionId(
        String localHost,
        int localPort,
        String remoteHost,
        int remotePort,
        int processorId,
        int index
    ) {
        this.localHost = localHost;
        this.localPort = localPort;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.processorId = processorId;
        this.index = index;
    }

    private ServerConnectionId(
        Map.Entry<String, Integer> localEndpoint,
        Map.Entry<String, Integer> remoteEndpoint,
        int processorId,
        int index
    ) {
        this(localEndpoint.getKey(), localEndpoint.getValue(), remoteEndpoint.getKey(), remoteEndpoint.getValue(), processorId, index);
    }

    public String localHost() {
        return localHost;
    }

    public int localPort() {
        return localPort;
    }

    public String remoteHost() {
        return remoteHost;
    }

    public int remotePort() {
        return remotePort;
    }

    public int processorId() {
        return processorId;
    }

    public int index() {
        return index;
    }

    /**
     * 从给定的连接ID字符串解析并创建ServerConnectionId对象
     * 
     * 实现细节：
     * 1. 首先按照'-'分割连接ID字符串，必须包含4个部分
     * 2. 分别解析本地和远程的host:port字符串
     * 3. 将processorId和index解析为整数
     * 4. 任何解析失败都会返回空Optional
     *
     * @param connectionIdString 要解析的连接ID字符串，格式："localHost:localPort-remoteHost:remotePort-processorId-index"
     * @return 包含ServerConnectionId对象的Optional，如果解析失败则返回空Optional
     */
    public static Optional<ServerConnectionId> fromString(String connectionIdString) {
        String[] split = connectionIdString.split("-");
        if (split.length != 4) {
            return Optional.empty();
        }

        try {
            return parseHostPort(split[0]).flatMap(localHost -> parseHostPort(split[1]).map(
                remoteHost -> new ServerConnectionId(localHost, remoteHost, Integer.parseInt(split[2]), Integer.parseInt(split[3]))));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    /**
     * 为给定的Socket生成唯一的连接ID
     * 
     * 实现细节：
     * 1. 从Socket中获取本地地址和端口
     * 2. 从Socket中获取远程地址和端口
     * 3. 使用指定的处理器ID和连接索引
     * 4. 按照标准格式拼接各个组件
     *
     * @param socket 需要生成连接ID的Socket对象
     * @param processorId 处理该连接的服务器处理器ID
     * @param connectionIndex 用于确保连接ID唯一性的索引值
     * @return 格式化的唯一连接ID字符串
     */
    public static String generateConnectionId(Socket socket, int processorId, int connectionIndex) {
        String localHost = socket.getLocalAddress().getHostAddress();
        int localPort = socket.getLocalPort();
        String remoteHost = socket.getInetAddress().getHostAddress();
        int remotePort = socket.getPort();
        return localHost + ":" + localPort + "-" + remoteHost + ":" + remotePort + "-" + processorId + "-" + connectionIndex;
    }

    /**
     * 解析host:port格式的字符串，支持IPv4和IPv6地址
     * 
     * 实现细节：
     * 1. 使用正则表达式匹配host:port格式
     * 2. 提取host部分（组1）和port部分（组2）
     * 3. 将port解析为整数
     * 4. 任何解析失败都会返回空Optional
     *
     * @param connectionString 要解析的连接字符串，格式："host:port"或"ipv6_host:port"
     * @return 包含主机地址和端口的Map.Entry的Optional，如果解析失败则返回空Optional
     */
    // 用于测试的可见性
    static Optional<Map.Entry<String, Integer>> parseHostPort(String connectionString) {
        Matcher matcher = HOST_PORT_PARSE_EXP.matcher(connectionString);
        if (matcher.matches()) {
            try {
                return Optional.of(Map.entry(matcher.group(1), Integer.parseInt(matcher.group(2))));
            } catch (NumberFormatException e) {
                // Ignore
            }
        }
        return Optional.empty();
    }
}
