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

package org.apache.kafka.server.authorizer;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.net.InetAddress;

/**
 * 请求上下文接口，为插件提供来自请求头的数据以及连接和身份验证信息。
 * <p>
 * 应用场景：
 * 该接口主要用于 Kafka 授权插件（Authorizer）。当 Kafka Broker 收到客户端请求时，
 * 在执行操作之前，会调用授权插件来判断该客户端是否有权限执行此操作。
 * 授权插件通过此接口获取请求的详细信息，例如请求来源、用户信息、请求类型等，
 * 以便做出准确的授权决策。
 * </p>
 * <p>
 * 实现细节：
 * Kafka Broker 在处理每个客户端请求时，会创建一个实现了此接口的对象实例。
 * 这个实例封装了当前请求的所有相关上下文信息。
 * 不同的 Kafka 版本或自定义实现可能会有不同的具体类来实现此接口，
 * 但它们都必须提供接口定义的方法。
 * </p>
 * <p>
 * 设计考虑：
 * 设计此接口的目的是为了解耦授权逻辑与核心请求处理逻辑。
 * 通过定义一个标准的上下文接口，Kafka 允许开发者编写自定义的授权插件，
 * 而无需关心 Kafka 内部请求处理的复杂细节。
 * 接口的 {@code @InterfaceStability.Evolving} 注解表明该接口仍在发展中，
 * 未来版本可能会有不兼容的变更，插件开发者需要注意这一点。
 * </p>
 */
@InterfaceStability.Evolving
public interface AuthorizableRequestContext {

    /**
     * 返回接收到请求的监听器的名称。
     * <p>
     * 应用场景：
     * 在一个 Kafka Broker 上可能配置了多个监听器（Listener），例如一个用于内部通信，
     * 另一个用于外部客户端连接，它们可能使用不同的端口或安全协议。
     * 授权插件可能需要根据请求来自哪个监听器来应用不同的授权策略。
     * 例如，来自内部监听器的请求可能拥有更高的权限。
     * </p>
     * <p>
     * 实现细节：
     * Kafka Broker 在接收到请求时，会确定该请求是通过哪个监听器进入的，
     * 并将该监听器的名称设置到请求上下文中。监听器的名称是在 Broker 配置中定义的。
     * </p>
     * <p>
     * 设计考虑：
     * 提供监听器名称使得授权策略可以更加灵活和细致。
     * 例如，可以针对特定的监听器配置更严格或更宽松的访问控制。
     * </p>
     * @return 监听器名称字符串
     */
    String listenerName();

    /**
     * 返回接收到请求的监听器的安全协议。
     * <p>
     * 应用场景：
     * Kafka 支持多种安全协议，如 PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL。
     * 授权插件可能需要根据连接使用的安全协议来调整授权逻辑。
     * 例如，对于使用 PLAINTEXT 协议的连接，可能需要更严格的审查，
     * 或者某些操作只允许通过 SSL/SASL_SSL 等加密协议进行。
     * </p>
     * <p>
     * 实现细节：
     * Broker 根据接收请求的监听器配置来确定其安全协议。
     * {@link org.apache.kafka.common.security.auth.SecurityProtocol} 是一个枚举类型，
     * 定义了 Kafka 支持的各种安全协议。
     * </p>
     * <p>
     * 设计考虑：
     * 将安全协议暴露给授权插件，有助于实现基于连接安全级别的授权。
     * 这增强了系统的安全性，允许管理员根据风险评估来配置不同的访问权限。
     * </p>
     * @return {@link SecurityProtocol} 枚举值，表示安全协议
     */
    SecurityProtocol securityProtocol();

    /**
     * 返回接收到请求的连接的已认证主体（principal）。
     * <p>
     * 应用场景：
     * 主体代表了已认证的用户或服务。授权决策的核心通常是基于这个主体的身份。
     * 例如，可以判断用户 "alice" 是否有权限读取主题 "orders"。
     * 如果连接是匿名的（例如，在 PLAINTEXT 协议下且未配置 SASL），
     * 则主体可能是预定义的匿名用户。
     * </p>
     * <p>
     * 实现细节：
     * 主体信息是在连接建立和身份验证阶段确定的。
     * 对于使用 SASL 的连接，主体通常是 SASL 用户名。
     * 对于使用 SSL 客户端证书认证的连接，主体可能是证书的 DN (Distinguished Name)。
     * {@link org.apache.kafka.common.security.auth.KafkaPrincipal} 对象封装了主体的类型和名称。
     * </p>
     * <p>
     * 设计考虑：
     * 提供认证后的主体是授权的基础。它使得授权插件能够识别请求的发起者，
     * 并据此查询 ACLs (Access Control Lists) 或其他权限存储来做出决策。
     * </p>
     * @return {@link KafkaPrincipal} 对象，表示已认证的用户或服务
     */
    KafkaPrincipal principal();

    /**
     * 返回发送请求的客户端的 IP 地址。
     * <p>
     * 应用场景：
     * 客户端 IP 地址可以用于多种授权目的，例如：
     * 1. 基于 IP 的访问控制：只允许特定 IP 段的客户端访问。
     * 2. 审计和日志记录：记录请求来源 IP，用于安全审计和问题排查。
     * 3. 风险评估：结合其他信息，评估来自特定 IP 的请求风险。
     * </p>
     * <p>
     * 实现细节：
     * Kafka Broker 从建立的网络连接中获取客户端的 IP 地址。
     * 返回的是 {@link java.net.InetAddress} 对象，可以从中获取 IP 地址的字符串表示。
     * </p>
     * <p>
     * 设计考虑：
     * 提供客户端 IP 地址为授权策略增加了另一维度。
     * 需要注意的是，如果 Kafka Broker 部署在负载均衡器或反向代理之后，
     * 这里获取到的可能是代理的 IP 地址，而不是原始客户端的 IP。
     * 在这种情况下，可能需要额外的配置（如 X-Forwarded-For 头）和处理来获取真实客户端 IP。
     * </p>
     * @return {@link InetAddress} 对象，表示客户端的 IP 地址
     */
    InetAddress clientAddress();

    /**
     * 返回请求头中的16位请求 API Key。
     * API Key 是一个整数，唯一标识了 Kafka 协议中的一种请求类型。
     * 例如，Produce 请求、Fetch 请求、CreateTopics 请求等都有各自的 API Key。
     * 更多关于 API Key 的信息，请参见 <a href="https://kafka.apache.org/protocol#protocol_api_keys">Kafka 协议文档</a>。
     * <p>
     * 应用场景：
     * 授权插件需要知道客户端尝试执行的具体操作类型，以便判断是否有相应的权限。
     * 例如，用户是否有权限发送消息 (ProduceRequest) 或创建主题 (CreateTopicsRequest)。
     * </p>
     * <p>
     * 实现细节：
     * Kafka 客户端在发送请求时，会在请求头中包含 API Key。
     * Broker 解析请求头后，将此 API Key 放入请求上下文中。
     * {@link org.apache.kafka.common.protocol.ApiKeys} 枚举类中定义了所有已知的 API Key 及其对应的名称和 ID。
     * </p>
     * <p>
     * 设计考虑：
     * 使用数字类型的 API Key 而不是字符串名称，是为了提高协议解析的效率。
     * 授权插件通常会将这个整数 API Key 映射到具体的操作类型（如 {@link org.apache.kafka.common.acl.AclOperation}）
     * 来进行权限检查。
     * </p>
     * @return 请求的 API Key (整数)
     */
    int requestType();

    /**
     * 返回请求头中的请求版本号。
     * <p>
     * 应用场景：
     * Kafka 协议是版本化的，同一个 API Key (请求类型) 可能有多个版本。
     * 不同版本的请求可能包含不同的字段或具有不同的行为。
     * 授权插件在某些高级场景下可能需要考虑请求版本，
     * 例如，某个操作的特定版本引入了新的敏感字段，需要更严格的授权。
     * </p>
     * <p>
     * 实现细节：
     * 客户端在发送请求时，会指定其使用的请求版本号。
     * Broker 根据此版本号来解析请求体和构造响应。
     * </p>
     * <p>
     * 设计考虑：
     * 暴露请求版本号使得授权逻辑可以适应协议的演进。
     * 大多数情况下，授权可能不直接依赖于请求版本，而是依赖于操作类型和资源。
     * 但在需要精细控制或处理协议兼容性问题时，此信息可能非常有用。
     * </p>
     * @return 请求版本号 (整数)
     */
    int requestVersion();

    /**
     * 返回请求头中的客户端 ID (client.id)。
     * <p>
     * 应用场景：
     * 客户端 ID 是由 Kafka 客户端在连接时指定的一个逻辑名称，用于标识客户端应用。
     * 它可以用于：
     * 1. 日志记录和监控：追踪特定客户端应用的行为。
     * 2. 配额管理：Kafka 可以基于 client.id 设置请求配额。
     * 3. 授权：虽然不常用作主要的授权依据（通常使用 principal），但在某些场景下，
     *    可以结合 client.id 进行更细粒度的控制或审计。
     * </p>
     * <p>
     * 实现细节：
     * 客户端通过其配置参数 (如 ProducerConfig.CLIENT_ID_CONFIG 或 ConsumerConfig.CLIENT_ID_CONFIG)
     * 来设置 client.id。这个 ID 会包含在发送给 Broker 的每个请求头中。
     * </p>
     * <p>
     * 设计考虑：
     * client.id 是用户自定义的字符串，其唯一性和格式没有严格保证 (除非用户自己管理)。
     * 因此，在安全敏感的授权决策中，应优先使用经过认证的 principal。
     * client.id 更多地用于标识和追踪。
     * </p>
     * @return 客户端 ID 字符串
     */
    String clientId();

    /**
     * 返回请求头中的关联 ID (correlation id)。
     * <p>
     * 应用场景：
     * 关联 ID 是由客户端生成的整数，用于将请求与对应的响应关联起来。
     * 客户端发送请求时会包含一个 correlation id，Broker 在处理完请求后，
     * 会在响应中返回相同的 correlation id，这样客户端就能匹配请求和响应。
     * 对于授权插件而言，correlation id 主要用于日志记录和调试，
     * 以便追踪单个请求的处理流程。它通常不直接用于授权决策。
     * </p>
     * <p>
     * 实现细节：
     * Kafka 客户端库会自动为每个请求生成一个唯一的 (在客户端视角) correlation id。
     * Broker 在响应中原样返回此 ID。
     * </p>
     * <p>
     * 设计考虑：
     * 暴露 correlation id 主要是为了方便问题排查和日志分析。
     * 授权插件可以将此 ID 包含在其日志条目中，从而能够将授权活动与特定的客户端请求联系起来。
     * </p>
     * @return 关联 ID (整数)
     */
    int correlationId();
}
