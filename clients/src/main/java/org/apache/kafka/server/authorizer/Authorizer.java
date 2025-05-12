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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

import java.io.Closeable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 *
 * 用于 Kafka broker 的可插拔授权者接口。
 *
 * Broker 中的启动顺序：
 * <ol>
 *   <li>如果 `authorizer.class.name` 中配置了授权者实例，Broker 会创建该实例。</li>
 *   <li>Broker 配置并启动授权者实例。授权者实现开始加载其元数据。</li>
 *   <li>Broker 启动 SocketServer 以接受连接并处理请求。</li>
 *   <li>对于每个监听器，SocketServer 会等待授权元数据在授权者中可用后才接受连接。{@link #start(AuthorizerServerInfo)} 为每个监听器返回的 future 必须仅在授权者准备好授权该监听器上的请求时才返回。</li>
 *   <li>Broker 接受连接。对于每个连接，Broker 执行身份验证，然后接受 Kafka 请求。
 *       对于每个请求，Broker 调用 {@link #authorize(AuthorizableRequestContext, List)} 来授权请求执行的操作。</li>
 * </ol>
 *
 * 授权者实现类可以选择性地实现 @{@link org.apache.kafka.common.Reconfigurable} 以支持动态重新配置而无需重新启动 Broker。
 * <p>
 * <b>线程模型：</b>
 * <ul>
 *   <li>所有授权者操作（包括授权和 ACL 更新）都必须是线程安全的。</li>
 *   <li>ACL 更新方法是异步的。更新延迟较低的实现可以使用 {@link java.util.concurrent.CompletableFuture#completedFuture(Object)} 返回一个已完成的 future。
 *       这确保了调用者将同步处理请求，而无需使用 purgatory 来等待结果。如果 ACL 更新需要可能阻塞的远程通信，
 *       则返回一个在远程操作完成时异步完成的 future。这使得调用者可以在不阻塞的情况下处理请求线程上的其他请求。</li>
 *   <li>用于异步处理远程操作的任何线程或线程池都可以在 {@link #start(AuthorizerServerInfo)} 期间启动。这些线程必须在 {@link Authorizer#close()} 期间关闭。</li>
 * </ul>
 * </p>
 */
@InterfaceStability.Evolving
public interface Authorizer extends Configurable, Closeable {

    /**
     * 开始加载授权元数据，并返回可用于等待每个监听器上授权请求的元数据可用的 future。
     * 每个监听器只有在其元数据可用并且授权者准备好开始授权该监听器上的请求后才会启动。
     *
     * <p><strong>应用场景:</strong> Broker 启动时调用此方法初始化授权者，并确保在接受客户端连接之前，授权逻辑已准备就绪。</p>
     * <p><strong>实现细节:</strong> 实现者需要在此方法中启动其元数据加载过程。对于每个配置的监听器 (Endpoint)，
     * 都应返回一个 {@link CompletionStage}。当特定监听器的授权逻辑准备就绪时（例如，相关的 ACLs 已加载），
     * 对应的 {@link CompletionStage} 应完成。Broker 将等待这些 Future 完成后才开始在该监听器上接受连接。</p>
     * <p><strong>设计考虑:</strong> 返回 {@code Map<Endpoint, ? extends CompletionStage<Void>>} 允许 Broker 并行地等待多个监听器的授权准备就绪。
     * 使用 {@link CompletionStage} 支持异步加载，避免阻塞 Broker 的主启动线程。</p>
     *
     * @param serverInfo Broker 的元数据，包括 Broker ID 和监听器端点。
     * @return 每个端点的 {@link CompletionStage}，当授权者准备好开始授权该监听器上的请求时完成。
     */
    Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo);

    /**
     * 授权指定的操作。操作的附加元数据在 `requestContext` 中指定。
     * <p>
     * 这是一个同步 API，设计用于本地缓存的 ACL。由于此方法在处理每个请求时在请求线程上调用，
     * 因此该方法的实现应避免耗时的远程通信，以免阻塞请求线程。
     *
     * <p><strong>应用场景:</strong> 每当 Broker 收到一个需要授权的客户端请求时（例如，生产消息、消费消息、创建主题等），都会调用此方法。</p>
     * <p><strong>实现细节:</strong> 实现者需要根据 `requestContext`（包含客户端信息、连接信息等）和 `actions`（包含要操作的资源和具体操作）列表，
     * 逐个判断每个 action 是否被授权。通常，这涉及到查询内部维护的 ACL 规则。返回的 {@link AuthorizationResult} 列表必须与输入的 `actions` 列表顺序一致。</p>
     * <p><strong>设计考虑:</strong> 同步设计是为了低延迟，假设 ACLs 存储在本地或可快速访问的缓存中。
     * 如果授权决策需要较长时间（例如，远程调用），可能会严重影响 Kafka 的性能。
     * 批量处理 `actions` 可以减少方法调用开销。</p>
     *
     * @param requestContext 请求上下文，包括请求类型、安全协议和监听器名称。
     * @param actions 正在授权的操作，包括每个操作的资源和操作类型。
     * @return 每个操作的授权结果列表，顺序与提供的操作列表相同。
     */
    List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions);

    /**
     * 创建新的 ACL 绑定。
     * <p>
     * 这是一个异步 API，使调用者能够避免在更新期间阻塞。该 API 的实现可以使用
     * {@link java.util.concurrent.CompletableFuture#completedFuture(Object)} 返回已完成的 future，
     * 以在请求线程上同步处理更新。
     *
     * <p><strong>应用场景:</strong> 当管理员或客户端通过 Kafka API 请求创建新的访问控制规则时，Broker 会调用此方法。</p>
     * <p><strong>实现细节:</strong> 实现者需要处理 `aclBindings` 列表中的每个 {@link AclBinding}，将其持久化或更新到 ACL 存储中。
     * 对于每个绑定，返回一个 {@link CompletionStage<AclCreateResult>}，表示创建操作的结果。
     * 如果创建成功，{@link AclCreateResult} 不应包含异常；如果失败，则应包含相应的异常。
     * `requestContext` 提供了操作发起者的信息，可用于审计或权限检查（例如，检查用户是否有权创建 ACL）。</p>
     * <p><strong>设计考虑:</strong> 异步设计允许 ACL 更新操作（可能涉及 I/O 或网络通信）在后台执行，而不会阻塞处理客户端请求的关键线程。
     * 返回 {@link CompletionStage} 列表，允许调用者独立跟踪每个 ACL 创建操作的状态。</p>
     *
     * @param requestContext 如果 ACL 是由 Broker 创建以处理客户端创建 ACL 的请求，则为请求上下文。
     * @param aclBindings 要创建的 ACL 绑定。
     *
     * @return 输入列表中每个 ACL 绑定的创建结果，顺序与输入列表相同。每个结果都作为
     *         一个 {@link CompletionStage} 返回，当结果可用时完成。
     */
    List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings);

    /**
     * 删除与提供的过滤器匹配的所有 ACL 绑定。
     * <p>
     * 这是一个异步 API，使调用者能够避免在更新期间阻塞。该 API 的实现可以使用
     * {@link java.util.concurrent.CompletableFuture#completedFuture(Object)} 返回已完成的 future，
     * 以在请求线程上同步处理更新。
     * <p>
     * 有关并发更新保证的详细信息，请参阅授权者实现文档。
     *
     * <p><strong>应用场景:</strong> 当管理员或客户端通过 Kafka API 请求删除现有的访问控制规则时，Broker 会调用此方法。</p>
     * <p><strong>实现细节:</strong> 实现者需要处理 `aclBindingFilters` 列表中的每个 {@link AclBindingFilter}。
     * 对于每个过滤器，它应该识别并删除所有匹配的 ACL 绑定。返回的 {@link CompletionStage<AclDeleteResult>}
     * 应包含有关删除操作的信息，例如哪些绑定被成功删除，哪些匹配但无法删除（可能由于错误）。
     * `requestContext` 提供了操作发起者的信息。</p>
     * <p><strong>设计考虑:</strong> 与 `createAcls` 类似，异步设计是为了避免阻塞关键线程。
     * 使用过滤器允许批量删除 ACL，这比逐个删除更有效率。
     * {@link AclDeleteResult} 提供了详细的反馈，有助于调用者了解删除操作的确切结果。</p>
     *
     * @param requestContext 如果 ACL 是由 Broker 删除以处理客户端删除 ACL 的请求，则为请求上下文。
     * @param aclBindingFilters 用于匹配要删除的 ACL 绑定的过滤器。
     *
     * @return 输入列表中每个过滤器的删除结果，顺序与输入列表相同。
     *         每个结果指示实际删除了哪些 ACL 绑定，以及任何匹配但无法删除的绑定。
     *         每个结果都作为一个 {@link CompletionStage} 返回，当结果可用时完成。
     */
    List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters);

    /**
     * 返回与提供的过滤器匹配的 ACL 绑定。
     * <p>
     * 这是一个同步 API，设计用于本地缓存的 ACL。此方法在处理 DescribeAcls 请求时在请求线程上调用，
     * 应避免耗时的远程通信，以免阻塞请求线程。
     *
     * <p><strong>应用场景:</strong> 当管理员或客户端通过 Kafka API 请求查询（描述）现有的访问控制规则时，Broker 会调用此方法。</p>
     * <p><strong>实现细节:</strong> 实现者需要根据提供的 `filter` 从其 ACL 存储中检索匹配的 {@link AclBinding}。
     * 返回一个 {@link Iterable}，允许调用者遍历匹配的绑定。返回迭代器而不是完整的列表，可以支持惰性加载和处理大量 ACLs 的情况，从而提高内存效率。</p>
     * <p><strong>设计考虑:</strong> 同步设计是因为描述 ACLs 通常被认为是快速操作，并且结果需要立即返回给客户端。
     * 假设 ACLs 存储在本地或可快速访问的缓存中。如果 ACLs 存储在远程且访问缓慢，则此操作可能会成为瓶颈。</p>
     *
     * @param filter 用于匹配 ACL 绑定的过滤器。
     * @return ACL 绑定的迭代器，可能惰性填充。
     */
    Iterable<AclBinding> acls(AclBindingFilter filter);

    /**
     * 获取当前 ACL 的数量，用于度量目的。未实现此函数的授权者将简单地返回 -1。
     *
     * <p><strong>应用场景:</strong> 用于监控和度量 Kafka 集群中 ACL 的数量。这有助于了解授权配置的复杂性。</p>
     * <p><strong>实现细节:</strong> 实现者应返回其当前管理的 ACL 总数。如果无法或不希望提供此计数，则可以依赖默认实现返回 -1。</p>
     * <p><strong>设计考虑:</strong> 这是一个可选的默认方法，为不需要此功能的授权者提供了便利。
     * 对于需要精确计数的实现，覆盖此方法很重要。</p>
     *
     * @return 当前 ACL 的数量，如果未实现则返回 -1。
     */
    default int aclCount() {
        // 实现者应返回当前 ACL 的总数。
        // 例如：return this.aclStore.size();
        return -1; // 默认情况下，如果授权者未覆盖此方法，则返回 -1，表示 ACL 数量未知或未实现计数功能。
    }

    /**
     * 检查调用者是否有权对给定类型的至少一个资源执行给定的 ACL 操作。
     *
     * <p><strong>用途:</strong> 此方法用于在不指定具体资源名称的情况下，判断用户是否对某一类资源拥有某种操作权限。
     * 例如，检查用户是否可以读取（READ）任何主题（TOPIC）。</p>
     *
     * <p><strong>应用场景:</strong>
     * <ul>
     *   <li>当 Kafka 客户端尝试执行某些需要广泛权限的操作时，例如列出所有主题，Broker 可以使用此方法进行初步权限检查。</li>
     *   <li>在某些管理操作中，可能需要确认用户是否对特定类型的资源拥有一般性的操作权限。</li>
     * </ul>
     * </p>
     *
     * <p><strong>实现细节:</strong></p>
     * <p>自定义授权者实现应考虑覆盖此默认实现，因为：</p>
     * <ol>
     *   <li>默认实现会多次迭代所有 AclBinding，没有按主体、主机、操作、权限类型和资源类型进行任何缓存。
     *       可以在自定义授权者中添加更高效的实现，直接访问缓存的条目。</li>
     *   <li>默认实现无法与授权者实现中包含的任何审计日志集成。</li>
     *   <li>默认实现不支持除 ACL 之外的任何自定义授权者配置或其他访问规则。</li>
     * </ol>
     * <p>该方法的默认实现逻辑如下：</p>
     * <ol>
     *   <li>首先，通过调用 `authorize` 方法检查一个硬编码的资源名称（"hardcode"），以确保超级用户无论是否存在 DENY ACL 都会被授予访问权限。如果允许，则直接返回 `ALLOWED`。这是为了确保超级用户的权限优先。</li>
     *   <li>创建一个 `ResourcePatternFilter` 来匹配指定的 `resourceType` 和任何模式类型（`PatternType.ANY`），以及一个 `AclBindingFilter` 来匹配此资源过滤器和任何访问控制条目过滤器（`AccessControlEntryFilter.ANY`）。</li>
     *   <li>初始化两个 `EnumMap`：`denyPatterns` 和 `allowPatterns`，用于分别存储匹配的 DENY 和 ALLOW 规则的资源名称。这两个 Map 都按 `PatternType` (LITERAL, PREFIXED) 分类存储。</li>
     *   <li>初始化一个布尔标志 `hasWildCardAllow` 为 `false`，用于跟踪是否存在通配符（WILDCARD_RESOURCE）的 ALLOW 规则。</li>
     *   <li>获取请求上下文中的主体 (`KafkaPrincipal`) 和客户端地址 (`hostAddr`)。</li>
     *   <li>遍历通过 `acls(aclFilter)` 获取的所有 ACL 绑定：
     *     <ul>
     *       <li>如果绑定的主机既不是客户端地址也不是通配符 "*"，则跳过此绑定。</li>
     *       <li>如果绑定的主体既不是请求主体也不是通配符用户 "User:*"，则跳过此绑定。</li>
     *       <li>如果绑定的操作既不是请求的操作也不是 `AclOperation.ALL`，则跳过此绑定。</li>
     *       <li>如果绑定的权限类型是 `AclPermissionType.DENY`：
     *         <ul>
     *           <li>如果模式类型是 `LITERAL`：
     *             <ul>
     *               <li>如果资源名称是通配符资源 (`ResourcePattern.WILDCARD_RESOURCE`)，则直接返回 `DENIED`，因为通配符 DENY 规则具有最高优先级。</li>
     *               <li>否则，将资源名称添加到 `denyPatterns` 的 `LITERAL` 集合中。</li>
     *             </ul>
     *           </li>
     *           <li>如果模式类型是 `PREFIXED`，则将资源名称添加到 `denyPatterns` 的 `PREFIXED` 集合中。</li>
     *           <li>然后继续处理下一个绑定。</li>
     *         </ul>
     *       </li>
     *       <li>如果绑定的权限类型不是 `AclPermissionType.ALLOW`，则跳过此绑定（因为已经处理了 DENY，其他类型不相关）。</li>
     *       <li>如果绑定的权限类型是 `AclPermissionType.ALLOW`：
     *         <ul>
     *           <li>如果模式类型是 `LITERAL`：
     *             <ul>
     *               <li>如果资源名称是通配符资源，则将 `hasWildCardAllow` 设置为 `true`，并继续处理下一个绑定。</li>
     *               <li>否则，将资源名称添加到 `allowPatterns` 的 `LITERAL` 集合中。</li>
     *             </ul>
     *           </li>
     *           <li>如果模式类型是 `PREFIXED`，则将资源名称添加到 `allowPatterns` 的 `PREFIXED` 集合中。</li>
     *         </ul>
     *       </li>
     *     </ul>
     *   </li>
     *   <li>如果 `hasWildCardAllow` 为 `true`，则返回 `ALLOWED`，因为通配符 ALLOW 规则（在没有通配符 DENY 的情况下）意味着允许访问任何该类型的资源。</li>
     *   <li>遍历 `allowPatterns` 中的所有条目（按 `PatternType` 分组）：
     *     <ul>
     *       <li>对于每个允许的资源名称 (`allowStr`)：
     *         <ul>
     *           <li>如果模式类型是 `LITERAL` 且 `denyPatterns` 的 `LITERAL` 集合中包含此 `allowStr`，则跳过此 `allowStr`（因为精确匹配的 DENY 规则覆盖了精确匹配的 ALLOW 规则）。</li>
     *           <li>创建一个 `StringBuilder` (`sb`) 和一个布尔标志 `hasDominatedDeny`（初始化为 `false`）。</li>
     *           <li>遍历 `allowStr` 的每个字符：
     *             <ul>
     *               <li>将字符追加到 `sb`。</li>
     *               <li>如果 `denyPatterns` 的 `PREFIXED` 集合中包含 `sb.toString()`（即当前前缀被 DENY 规则覆盖），则将 `hasDominatedDeny` 设置为 `true` 并中断内部循环。</li>
     *             </ul>
     *           </li>
     *           <li>如果 `hasDominatedDeny` 为 `false`（即此 ALLOW 规则没有被任何更具体的 DENY 规则或前缀 DENY 规则覆盖），则返回 `ALLOWED`。</li>
     *         </ul>
     *       </li>
     *     </ul>
     *   </li>
     *   <li>如果遍历完所有 ALLOW 规则后都没有返回 `ALLOWED`，则最终返回 `DENIED`。</li>
     * </ol>
     *
     * <p><strong>设计考虑:</strong></p>
     * <ul>
     *   <li><strong>性能:</strong> 默认实现可能效率不高，因为它会多次迭代所有 ACL。对于包含大量 ACL 的系统，自定义实现应考虑使用更优化的数据结构和缓存策略。</li>
     *   <li><strong>超级用户优先:</strong> 通过硬编码的资源名称检查，确保超级用户始终拥有权限，这是一种常见的安全设计模式。</li>
     *   <li><strong>规则优先级:</strong> DENY 规则通常优先于 ALLOW 规则。通配符 DENY 规则具有很高的否决权。精确匹配的 DENY 会覆盖精确匹配的 ALLOW。前缀 DENY 会覆盖其范围内的 ALLOW。</li>
     *   <li><strong>可扩展性:</strong> 鼓励用户覆盖此方法以实现更高效或更符合特定需求的授权逻辑，例如集成审计日志或支持自定义配置。</li>
     * </ul>
     *
     * @param requestContext 请求上下文，包括请求资源类型、安全协议和监听器名称
     * @param op 要检查的 ACL 操作
     * @param resourceType 要检查的资源类型
     * @return 如果调用者有权对给定类型的至少一个资源执行给定的 ACL 操作，则返回 {@link AuthorizationResult#ALLOWED}。
     *         否则返回 {@link AuthorizationResult#DENIED}。
     */
    default AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType) {
        // 验证 authorizeByResourceType 方法的参数 op 和 resourceType 是否有效。
        SecurityUtils.authorizeByResourceTypeCheckArgs(op, resourceType);

        // 检查一个硬编码的名称（"hardcode"）以确保超级用户被授予访问权限，无论是否存在 DENY ACL。
        // 这是为了确保超级用户总是拥有权限。
        // 创建一个针对特定操作（op）和资源类型（resourceType）的 Action 对象，资源名称硬编码为 "hardcode"，模式为 LITERAL。
        // 0, true, false 这些参数分别代表：logIfAllowed=false, logIfDenied=true, silent=false (这些是 Action 构造函数的旧参数，现在可能已更改或有不同含义)
        // 调用 authorize 方法检查此 Action 是否被允许。
        if (authorize(requestContext, Collections.singletonList(new Action(
                op, new ResourcePattern(resourceType, "hardcode", PatternType.LITERAL),
                0, true, false))) // 假设 Action 构造函数的最后三个参数是：resourceReferenceCount, logIfAllowed, logIfDenied
                .get(0) == AuthorizationResult.ALLOWED) { // 如果对硬编码资源的授权结果是 ALLOWED
            return AuthorizationResult.ALLOWED; // 直接返回 ALLOWED，表明超级用户检查通过
        }

        // 创建资源模式过滤器，用于过滤出与指定资源类型（resourceType）相关的资源模式。
        // resourceName 设置为 null，表示匹配任何资源名称。
        // patternType 设置为 PatternType.ANY，表示匹配任何模式类型（LITERAL, PREFIXED, ANY, MATCH, UNKNOWN）。
        ResourcePatternFilter resourceTypeFilter = new ResourcePatternFilter(
            resourceType, null, PatternType.ANY);
        // 创建 ACL 绑定过滤器，基于上述 resourceTypeFilter 和一个允许任何访问控制条目的过滤器。
        // AccessControlEntryFilter.ANY 表示匹配任何用户、主机、操作和权限类型的 ACL 条目。
        AclBindingFilter aclFilter = new AclBindingFilter(
            resourceTypeFilter, AccessControlEntryFilter.ANY);

        // 初始化一个 EnumMap 用于存储 DENY 规则的资源模式名称。
        //键是 PatternType (LITERAL, PREFIXED)，值是包含资源名称的 Set。
        EnumMap<PatternType, Set<String>> denyPatterns =
            new EnumMap<>(PatternType.class) {{ // 使用双括号初始化（匿名内部类 + 实例初始化块）
                    put(PatternType.LITERAL, new HashSet<>()); // 存储字面量匹配的 DENY 规则
                    put(PatternType.PREFIXED, new HashSet<>()); // 存储前缀匹配的 DENY 规则
                }};
        // 初始化一个 EnumMap 用于存储 ALLOW 规则的资源模式名称。
        // 结构与 denyPatterns 相同。
        EnumMap<PatternType, Set<String>> allowPatterns =
            new EnumMap<>(PatternType.class) {{ // 使用双括号初始化
                    put(PatternType.LITERAL, new HashSet<>()); // 存储字面量匹配的 ALLOW 规则
                    put(PatternType.PREFIXED, new HashSet<>()); // 存储前缀匹配的 ALLOW 规则
                }};

        //布尔标志，用于记录是否存在通配符（*）的 ALLOW 规则。
        boolean hasWildCardAllow = false;

        // 从请求上下文中获取 Kafka 主体（用户）。
        KafkaPrincipal principal = new KafkaPrincipal(
            requestContext.principal().getPrincipalType(), // 获取主体的类型 (例如 User)
            requestContext.principal().getName()); // 获取主体的名称
        // 从请求上下文中获取客户端的 IP 地址。
        String hostAddr = requestContext.clientAddress().getHostAddress();

        // 遍历所有与 aclFilter 匹配的 ACL 绑定。
        // acls(aclFilter) 方法会返回符合过滤条件的 AclBinding 集合。
        for (AclBinding binding : acls(aclFilter)) {
            // 检查 ACL 条目中的主机是否与请求的客户端地址匹配，或者是否为通配符 "*"。
            if (!binding.entry().host().equals(hostAddr) && !binding.entry().host().equals("*"))
                continue; // 如果主机不匹配且不是通配符，则跳过此 ACL 绑定。

            // 检查 ACL 条目中的主体是否与请求的主体匹配，或者是否为通配符用户 "User:*"。
            // SecurityUtils.parseKafkaPrincipal 用于将 ACL 条目中的主体字符串转换为 KafkaPrincipal 对象。
            if (!SecurityUtils.parseKafkaPrincipal(binding.entry().principal()).equals(principal)
                    && !binding.entry().principal().equals("User:*"))
                continue; // 如果主体不匹配且不是通配符用户，则跳过此 ACL 绑定。

            // 检查 ACL 条目中的操作是否与请求的操作匹配，或者是否为 AclOperation.ALL (所有操作)。
            if (binding.entry().operation() != op
                    && binding.entry().operation() != AclOperation.ALL)
                continue; // 如果操作不匹配且不是 ALL，则跳过此 ACL 绑定。

            // 如果 ACL 条目的权限类型是 DENY。
            if (binding.entry().permissionType() == AclPermissionType.DENY) {
                // 根据资源模式的类型处理 DENY 规则。
                switch (binding.pattern().patternType()) {
                    case LITERAL: // 如果是字面量匹配
                        // 如果 DENY 规则的资源名称是通配符资源 (ResourcePattern.WILDCARD_RESOURCE，通常是 "*")。
                        if (binding.pattern().name().equals(ResourcePattern.WILDCARD_RESOURCE))
                            return AuthorizationResult.DENIED; // 通配符 DENY 规则直接导致拒绝访问。
                        // 将字面量 DENY 规则的资源名称添加到 denyPatterns 中。
                        denyPatterns.get(PatternType.LITERAL).add(binding.pattern().name());
                        break;
                    case PREFIXED: // 如果是前缀匹配
                        // 将前缀 DENY 规则的资源名称添加到 denyPatterns 中。
                        denyPatterns.get(PatternType.PREFIXED).add(binding.pattern().name());
                        break;
                    default: // 其他模式类型（如 ANY, MATCH, UNKNOWN）在此上下文中不特殊处理或被忽略
                }
                continue; // 处理完 DENY 规则后，继续下一个 ACL 绑定。
            }

            // 如果 ACL 条目的权限类型不是 ALLOW (因为 DENY 已经处理，其他类型如 UNKNOWN 在此不考虑)。
            if (binding.entry().permissionType() != AclPermissionType.ALLOW)
                continue; // 跳过此 ACL 绑定。

            // 如果 ACL 条目的权限类型是 ALLOW。
            switch (binding.pattern().patternType()) {
                case LITERAL: // 如果是字面量匹配
                    // 如果 ALLOW 规则的资源名称是通配符资源。
                    if (binding.pattern().name().equals(ResourcePattern.WILDCARD_RESOURCE)) {
                        hasWildCardAllow = true; // 标记存在通配符 ALLOW 规则。
                        continue; // 继续下一个 ACL 绑定，因为通配符 ALLOW 的处理在循环之后。
                    }
                    // 将字面量 ALLOW 规则的资源名称添加到 allowPatterns 中。
                    allowPatterns.get(PatternType.LITERAL).add(binding.pattern().name());
                    break;
                case PREFIXED: // 如果是前缀匹配
                    // 将前缀 ALLOW 规则的资源名称添加到 allowPatterns 中。
                    allowPatterns.get(PatternType.PREFIXED).add(binding.pattern().name());
                    break;
                default: // 其他模式类型在此上下文中不特殊处理或被忽略
            }
        }

        // 如果在处理 ACL 绑定时发现了通配符 ALLOW 规则。
        if (hasWildCardAllow) {
            return AuthorizationResult.ALLOWED; // 存在通配符 ALLOW 规则（且没有被通配符 DENY 覆盖），则允许访问。
        }

        // 对于任何字面量 ALLOW 规则，如果没有显式的字面量 DENY 或前缀 DENY 规则覆盖它，则允许。
        // 对于任何前缀 ALLOW 规则，如果没有显式的前缀 DENY 规则覆盖它，则允许。
        // 遍历 allowPatterns 中的每个条目（PatternType -> Set<String>）。
        for (Map.Entry<PatternType, Set<String>> entry : allowPatterns.entrySet()) {
            // 遍历当前模式类型下的所有允许的资源名称字符串。
            for (String allowStr : entry.getValue()) {
                // 如果当前 ALLOW 规则是字面量类型，并且 denyPatterns 的字面量集合中也包含这个资源名称。
                if (entry.getKey() == PatternType.LITERAL
                        && denyPatterns.get(PatternType.LITERAL).contains(allowStr))
                    continue; // 精确匹配的 DENY 规则覆盖了精确匹配的 ALLOW 规则，跳过此 ALLOW 规则。
                
                // 检查是否存在一个前缀 DENY 规则，其前缀是当前 allowStr 的子串。
                StringBuilder sb = new StringBuilder(); // 用于构建 allowStr 的前缀。
                boolean hasDominatedDeny = false; // 标记是否存在一个覆盖此 ALLOW 规则的前缀 DENY。
                // 遍历 allowStr 的每个字符，构建其所有可能的前缀。
                for (char ch : allowStr.toCharArray()) {
                    sb.append(ch); // 将字符追加到 StringBuilder。
                    // 如果 denyPatterns 的前缀集合中包含当前构建的前缀。
                    if (denyPatterns.get(PatternType.PREFIXED).contains(sb.toString())) {
                        hasDominatedDeny = true; // 找到了一个覆盖此 ALLOW 规则的前缀 DENY。
                        break; // 无需再检查更长的前缀。
                    }
                }
                // 如果没有找到任何覆盖此 ALLOW 规则的前缀 DENY。
                if (!hasDominatedDeny)
                    return AuthorizationResult.ALLOWED; // 则此 ALLOW 规则生效，允许访问。
            }
        }

        // 如果遍历了所有 ALLOW 规则后都没有找到一个有效的（未被 DENY 覆盖的）ALLOW 规则。
        return AuthorizationResult.DENIED; // 最终返回 DENIED。
    }

}
