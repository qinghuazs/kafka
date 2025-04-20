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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.util.List;

/**
 * 为{@link NetworkClient}提供额外的工具方法，主要用于实现阻塞行为。
 * 这个工具类将非阻塞的NetworkClient包装成可以进行阻塞操作的形式。
 */
public final class NetworkClientUtils {

    // 私有构造函数，防止实例化
    private NetworkClientUtils() {}

    /**
     * 检查节点是否当前已连接。首先调用client.poll确保处理任何待处理的断开连接。
     * 
     * 该方法可用于在调用阻塞版本之前检查连接状态，以判断后者是否完成了新的连接。
     * 
     * @param client Kafka客户端实例
     * @param node 要检查的节点
     * @param currentTime 当前时间戳（毫秒）
     * @return 如果节点已连接并就绪则返回true，否则返回false
     */
    public static boolean isReady(KafkaClient client, Node node, long currentTime) {
        // 调用poll(0)处理任何待处理的断开连接事件
        client.poll(0, currentTime);
        // 检查节点是否就绪
        return client.isReady(node, currentTime);
    }

    /**
     * 调用client.poll处理待处理的断开连接，然后调用client.ready和多次client.poll，
     * 直到与node的连接就绪、超时或连接失败。
     * 
     * 返回值说明：
     * - 如果连接成功建立，返回true
     * - 如果超时，返回false
     * - 如果连接失败，抛出IOException
     * - 如果认证失败，抛出AuthenticationException
     * 
     * 注意：如果NetworkClient配置了正数的连接超时时间，对于最近断开的连接，
     * 该方法可能会抛出IOException。
     * 
     * 该方法用于在非阻塞的NetworkClient之上实现阻塞行为，使用时需谨慎。
     * 
     * @param client Kafka客户端实例
     * @param node 要连接的节点
     * @param time 时间工具类实例
     * @param timeoutMs 超时时间（毫秒）
     * @return 连接成功返回true，超时返回false
     * @throws IOException 连接失败时抛出
     * @throws AuthenticationException 认证失败时抛出
     */
    public static boolean awaitReady(KafkaClient client, Node node, Time time, long timeoutMs) throws IOException {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("Timeout needs to be greater than 0");
        }
        long startTime = time.milliseconds();

        if (isReady(client, node, startTime) ||  client.ready(node, startTime))
            return true;

        long attemptStartTime = time.milliseconds();
        while (!client.isReady(node, attemptStartTime) && attemptStartTime - startTime < timeoutMs) {
            if (client.connectionFailed(node)) {
                throw new IOException("Connection to " + node + " failed.");
            }
            long pollTimeout = timeoutMs - (attemptStartTime - startTime); // initialize in this order to avoid overflow

            // If the network client is waiting to send data for some reason (eg. throttling or retry backoff),
            // polling longer than that is potentially dangerous as the producer will not attempt to send
            // any pending requests.
            long waitingTime = client.pollDelayMs(node, startTime);
            if (waitingTime > 0 && pollTimeout > waitingTime) {
                // Block only until the next-scheduled time that it's okay to send data to the producer,
                // wake up, and try again. This is the way.
                pollTimeout = waitingTime;
            }

            client.poll(pollTimeout, attemptStartTime);
            if (client.authenticationException(node) != null)
                throw client.authenticationException(node);
            attemptStartTime = time.milliseconds();
        }
        return client.isReady(node, attemptStartTime);
    }

    /**
     * 调用client.send发送请求，然后通过一次或多次client.poll等待接收响应。
     * 如果发生断开连接（可能由于请求超时等多种原因），或者在方法执行期间客户端被关闭，
     * 将抛出IOException。
     * 
     * 这个方法实现了同步请求-响应模式，将异步的NetworkClient包装成同步调用的形式。
     * 使用时需要注意，因为它会阻塞直到收到响应或发生错误。
     * 
     * @param client Kafka客户端实例
     * @param request 要发送的请求
     * @param time 时间工具类实例
     * @return 服务器的响应
     * @throws IOException 当连接断开或客户端关闭时抛出
     */
    public static ClientResponse sendAndReceive(KafkaClient client, ClientRequest request, Time time) throws IOException {
        try {
            client.send(request, time.milliseconds());
            while (client.active()) {
                List<ClientResponse> responses = client.poll(Long.MAX_VALUE, time.milliseconds());
                for (ClientResponse response : responses) {
                    if (response.requestHeader().correlationId() == request.correlationId()) {
                        if (response.wasDisconnected()) {
                            throw new IOException("Connection to " + response.destination() + " was disconnected before the response was read");
                        }
                        if (response.versionMismatch() != null) {
                            throw response.versionMismatch();
                        }
                        return response;
                    }
                }
            }
            throw new IOException("Client was shutdown before response was read");
        } catch (DisconnectException e) {
            if (client.active())
                throw e;
            else
                throw new IOException("Client was shutdown before response was read");

        }
    }

    /**
     * 检查节点是否断开连接且当前无法立即重连（即处于重连退避窗口期）。
     * 
     * 当连接失败后，Kafka客户端会采用退避策略进行重连，在退避时间内不会尝试重新连接，
     * 这个方法用于检查节点是否处于这种状态。
     * 
     * @param client Kafka客户端实例
     * @param node 要检查的节点
     * @param time 时间工具类实例
     * @return 如果节点断开且在退避期内返回true，否则返回false
     */
    public static boolean isUnavailable(KafkaClient client, Node node, Time time) {
        // 检查连接是否失败且有正数的重连延迟时间
        return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
    }

    /**
     * 检查指定节点是否存在认证错误，如果存在则抛出该异常。
     * 
     * 这个方法用于主动检查和处理认证失败的情况，通常在建立连接或发送请求前调用，
     * 以确保没有未处理的认证错误。
     * 
     * @param client Kafka客户端实例
     * @param node 要检查的节点
     * @throws AuthenticationException 如果存在认证错误则抛出
     */
    public static void maybeThrowAuthFailure(KafkaClient client, Node node) {
        // 获取节点的认证异常（如果有）
        AuthenticationException exception = client.authenticationException(node);
        // 如果存在认证异常则抛出
        if (exception != null)
            throw exception;
    }

    /**
     * 尝试发起连接（如果当前可以连接）。
     * 这个方法主要用于重置套接字的失败状态，当之前的连接失败后，
     * 可以通过这个方法尝试重新建立连接。
     * 
     * @param client Kafka客户端实例
     * @param node 要连接的节点
     * @param time 时间工具类实例
     */
    public static void tryConnect(KafkaClient client, Node node, Time time) {
        // 调用ready方法尝试建立连接
        client.ready(node, time.milliseconds());
    }
}
