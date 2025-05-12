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

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;

/**
 * 在启动期间提供给授权者的运行时 Broker 配置元数据。
 * <p>
 * 此接口定义了授权者 (Authorizer) 在 Kafka Broker 启动时可以获取的 Broker 相关配置和状态信息。
 * 授权者是 Kafka 中用于控制客户端对资源访问权限的组件。通过此接口，自定义的授权者可以获取到
 * 其运行环境的上下文信息，例如集群ID、Broker ID、监听器端点等，从而能够做出更精细和动态的授权决策。
 * </p>
 * <p>
 * <b>应用场景:</b>
 * <ul>
 *   <li>自定义授权者在初始化时，需要根据当前 Broker 的角色或配置来加载特定的授权策略。</li>
 *   <li>授权者在进行权限校验时，可能需要参考集群信息或 Broker 的网络端点。</li>
 *   <li>审计日志记录时，可以将操作关联到具体的 Broker 实例和集群。</li>
 * </ul>
 * </p>
 * <p>
 * <b>设计考虑:</b>
 * <ul>
 *   <li>提供一个标准接口，使得不同的授权者实现可以以统一的方式访问 Broker 信息，增强了系统的可扩展性和模块化。</li>
 *   <li>将 Broker 的核心信息暴露给授权者，同时避免暴露过多的内部实现细节。</li>
 *   <li>通过 {@link org.apache.kafka.common.annotation.InterfaceStability.Evolving} 注解标明接口的稳定性，提醒开发者API可能发生变化。</li>
 * </ul>
 * </p>
 */
@InterfaceStability.Evolving // 此注解表示该接口目前处于演进阶段，意味着它的API可能会在未来的版本中发生变化，但会尽量保持向后兼容。使用者应该意识到这一点，并在升级Kafka版本时关注相关的变更。
public interface AuthorizerServerInfo {

    /**
     * 返回运行此授权者的 Broker 的集群元数据，包括集群 ID。
     * <p>
     * 集群 ID 是 Kafka 集群的唯一标识符。授权者可以使用此信息来区分不同的 Kafka 集群，
     * 例如，在多集群环境中应用不同的授权策略，或者在审计日志中记录操作发生的集群。
     * </p>
     * <p>
     * <b>应用场景:</b>
     * <ul>
     *   <li>当授权策略需要根据特定的集群进行调整时。</li>
     *   <li>记录审计信息，指明事件发生的具体集群。</li>
     * </ul>
     * </p>
     * <p>
     * <b>实现细节:</b>
     *   具体的实现类会负责从 Broker 的配置或运行时状态中获取集群ID，并将其封装在 {@link org.apache.kafka.common.ClusterResource} 对象中返回。
     *   集群ID通常在首次启动 Kafka 集群时生成并持久化。
     * </p>
     * <p>
     * <b>设计考虑:</b>
     *   将集群信息抽象为一个资源对象 ({@link org.apache.kafka.common.ClusterResource})，便于未来在不破坏接口兼容性的前提下扩展更多集群相关的元数据。
     * </p>
     * @return {@link org.apache.kafka.common.ClusterResource} 集群资源对象，包含了集群的唯一标识符。
     */
    ClusterResource clusterResource();

    /**
     * 返回 Broker ID。如果 {@code broker.id} 未配置，则这可能是一个生成的 Broker ID。
     * <p>
     * Broker ID 是 Kafka 集群中每个 Broker 实例的唯一数字标识。
     * 如果在 Broker 配置文件中显式设置了 {@code broker.id}，则返回该值。
     * 如果未配置，Kafka 会自动生成一个 ID。
     * </p>
     * <p>
     * <b>应用场景:</b>
     * <ul>
     *   <li>授权者可能需要基于 Broker ID 来区分不同的 Broker 实例，例如在多 Broker 环境中应用针对特定 Broker 的访问控制规则。</li>
     *   <li>用于日志记录和问题追踪，精确定位到发生事件的 Broker。</li>
     * </ul>
     * </p>
     * <p>
     * <b>实现细节:</b>
     *   实现类会首先尝试读取 Broker 配置中的 {@code broker.id}。如果该配置项不存在或无效，
     *   Broker 启动过程中会为其分配一个动态生成的 ID。此方法返回的即是最终确定的 Broker ID。
     * </p>
     * <p>
     * <b>设计考虑:</b>
     *   提供一个明确的 Broker 标识符，即使在 {@code broker.id} 未显式配置的情况下也能保证唯一性，这对于集群管理和授权至关重要。
     * </p>
     * @return Broker 的整数 ID。
     */
    int brokerId();

    /**
     * 返回所有监听器的端点，包括监听器绑定的宣告主机和端口。
     * <p>
     * Kafka Broker 可以配置多个监听器 (listeners)，每个监听器监听在不同的网络接口和端口上，
     * 并可能使用不同的安全协议 (如 PLAINTEXT, SSL, SASL_SSL)。此方法返回所有这些监听器的网络端点信息。
     * 每个端点 ({@link org.apache.kafka.common.Endpoint}) 包含了监听器名称、安全协议、主机名和端口号。
     * </p>
     * <p>
     * <b>应用场景:</b>
     * <ul>
     *   <li>授权者需要了解 Broker 提供了哪些网络接入点，以便进行更细致的连接控制。例如，可以根据客户端连接的监听器来应用不同的授权策略。</li>
     *   <li>审计连接请求时，记录客户端连接的具体端点。</li>
     *   <li>某些高级授权逻辑可能需要检查客户端尝试连接的端点是否与其声明的身份或网络来源相匹配。</li>
     * </ul>
     * </p>
     * <p>
     * <b>实现细节:</b>
     *   实现类会解析 Broker 配置文件中的 {@code listeners} 和 {@code advertised.listeners} (如果配置) 配置项，
     *   为每个有效的监听器创建一个 {@link org.apache.kafka.common.Endpoint} 对象，并将其收集到返回的集合中。
     * </p>
     * <p>
     * <b>设计考虑:</b>
     *   以集合形式提供所有端点，使得授权者可以遍历并处理所有可用的连接点，从而实现灵活的网络访问控制。
     *   返回的是 {@link org.apache.kafka.common.Endpoint} 对象的集合，这是一个定义良好的数据结构，包含了端点的所有必要信息。
     * </p>
     * @return 一个包含所有 {@link org.apache.kafka.common.Endpoint} 对象的集合。
     */
    Collection<Endpoint> endpoints();

    /**
     * 返回 Broker 间通信端点。这是 {@link #endpoints()} 返回的端点之一。
     * <p>
     * 在 Kafka 集群中，Broker 之间需要相互通信以进行数据复制、元数据同步等操作。
     * 这个通信通常通过一个专门配置的监听器进行，即 Broker 间监听器 (inter-broker listener)。
     * 此方法返回代表该 Broker 间监听器的 {@link org.apache.kafka.common.Endpoint} 对象。
     * </p>
     * <p>
     * <b>应用场景:</b>
     * <ul>
     *   <li>授权者可能需要对 Broker 间的内部通信应用不同于客户端连接的授权策略。例如，内部通信可能被认为是可信的，从而应用更宽松的规则。</li>
     *   <li>识别并区分内部流量和外部客户端流量，用于监控或安全审计。</li>
     * </ul>
     * </p>
     * <p>
     * <b>实现细节:</b>
     *   实现类会根据 Broker 配置中的 {@code inter.broker.listener.name} 来确定哪个监听器用于 Broker 间通信。
     *   然后从 {@link #endpoints()} 返回的集合中找到对应的 {@link org.apache.kafka.common.Endpoint} 对象。
     *   如果未配置 {@code inter.broker.listener.name}，行为可能取决于 Kafka 的版本和默认配置。
     * </p>
     * <p>
     * <b>设计考虑:</b>
     *   明确区分 Broker 间通信端点，有助于实现更精细的安全控制。这允许管理员为内部系统通信和外部客户端访问配置不同的安全级别和授权规则。
     * </p>
     * @return 用于 Broker 之间内部通信的 {@link org.apache.kafka.common.Endpoint} 对象。
     */
    Endpoint interBrokerEndpoint();

    /**
     * 返回配置的早期启动监听器。
     * <p>
     * 早期启动监听器 (early start listeners) 允许某些特定的监听器在 Kafka Broker 的核心服务（如Controller选举、分区加载等）
     * 完全初始化并准备好处理客户端请求之前就开始接受连接。这对于某些需要在 Broker 启动早期阶段就能与之交互的组件（例如，某些类型的监控或管理工具，或者授权插件本身）非常有用。
     * </p>
     * <p>
     * <b>应用场景:</b>
     * <ul>
     *   <li>授权插件可能需要在 Broker 启动的非常早期阶段就可用，以便在其他组件开始通信之前建立安全上下文或加载策略。</li>
     *   <li>某些特定的管理或监控工具可能需要连接到这些早期启动的监听器来获取 Broker 的初始状态信息。</li>
     * </ul>
     * </p>
     * <p>
     * <b>实现细节:</b>
     *   实现类会读取 Broker 配置中通过 {@code early.start.listeners} (或类似名称，具体配置项需查阅对应Kafka版本文档) 指定的监听器名称列表。
     *   这些名称对应于在 {@code listeners} 中定义的监听器。
     * </p>
     * <p>
     * <b>设计考虑:</b>
     *   提供早期启动监听器的能力，增加了 Broker 启动过程的灵活性，允许关键的基础设施组件（如安全相关的插件）尽早初始化并开始工作。
     *   这有助于确保即使在 Broker 启动的关键阶段，安全策略也能得到执行。
     * </p>
     * @return 一个包含早期启动监听器名称的字符串集合。如果未配置早期启动监听器，则返回空集合。
     */
    Collection<String> earlyStartListeners();
}
