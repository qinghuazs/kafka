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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

/**
 * 一个有状态的对象，用于跟踪单个成员与组相关的状态：
 * <p/>
 * 负责：
 * <ul>
 *   <li>维护成员状态</li>
 *   <li>维护成员的分配</li>
 *   <li>如果成员需要，则为组计算分配</li>
 * </ul>
 * 类变量 R 是特定组心跳 RPC 的响应。
 */
public abstract class AbstractMembershipManager<R extends AbstractResponse> implements RequestManager {
    // 类定义：抽象成员资格管理器，负责管理消费者组成员的生命周期和状态。

    /**
     * 基于主题名称和分区的 TopicPartition 比较器。
     * 应用场景：用于对 TopicPartition 集合进行排序，确保处理顺序的一致性。
     * 实现细节：使用 Kafka 提供的 {@link Utils.TopicPartitionComparator} 实现。
     * 设计考虑：提供一个标准的比较器，以便在需要排序 TopicPartition 的地方统一使用。
     */
    static final Utils.TopicPartitionComparator TOPIC_PARTITION_COMPARATOR = new Utils.TopicPartitionComparator(); // 字段：主题分区比较器，用于按主题名和分区号排序。

    /**
     * 基于主题名称和分区的 TopicIdPartition 比较器（在排序时忽略主题 ID，
     * 因为这主要用于日志记录目的）。
     * 应用场景：用于对 TopicIdPartition 集合进行排序，主要用于日志输出，方便阅读。
     * 实现细节：使用 Kafka 提供的 {@link Utils.TopicIdPartitionComparator} 实现，该比较器在比较时会忽略 Topic ID。
     * 设计考虑：在日志中展示分区信息时，通常更关心主题名和分区号，Topic ID 主要用于内部标识。
     */
    static final Utils.TopicIdPartitionComparator TOPIC_ID_PARTITION_COMPARATOR = new Utils.TopicIdPartitionComparator(); // 字段：主题ID分区比较器，用于按主题名和分区号排序（忽略主题ID），主要用于日志记录。

    /**
     * 成员将加入的消费者组的组 ID，在创建当前成员资格管理器时提供。
     * 应用场景：标识消费者所属的消费组，是消费者加入组协议的基础。
     * 实现细节：在构造函数中初始化，并且是 final 类型，一旦设置不可更改。
     * 设计考虑：确保成员管理器始终与一个特定的消费组关联。
     */
    protected final String groupId; // 字段：消费组ID。

    /**
     * 消费者在启动时生成的成员 ID，在组内唯一，并在进程的整个生命周期内保持一致。
     * 此 ID充当消费者进程的化身标识符，即使消费者离开并重新加入组，也不会重置或更改。
     * 成员 ID 保持不变，直到进程完全停止或终止。
     * 应用场景：唯一标识一个消费者实例，用于服务器端跟踪成员状态和分配。
     * 实现细节：使用 {@link Uuid#randomUuid()} 生成一个随机的 UUID 作为成员 ID，并转换为字符串。它是 final 类型，确保在实例生命周期内不变。
     * 设计考虑：需要一个在消费者进程重启前都保持稳定的唯一标识，以支持如静态成员资格等功能。
     */
    protected final String memberId = Uuid.randomUuid().toString(); // 字段：成员ID，由消费者启动时随机生成，全局唯一。

    /**
     * 成员的当前 epoch。成员将其设置为 0，并在心跳请求中提供给服务器以加入组。
     * 然后由服务器维护，当成员协调并确认其收到的分配时递增。如果成员被隔离(fenced)，它将被重置为 0。
     * 应用场景：用于防止脑裂和确保成员状态的一致性。服务器通过 epoch 来区分成员的不同“任期”。
     * 实现细节：初始化为0。当成员加入组或被隔离时，会进行更新。
     * 设计考虑：Epoch 是一个关键机制，用于处理成员资格变更和确保分配的正确性。
     */
    protected int memberEpoch = 0; // 字段：成员的纪元（epoch），用于跟踪成员资格的代数，防止旧的或无效的成员操作。初始值为0。

    /**
     * 此成员作为消费者组成员的当前状态，定义在 {@link MemberState} 中。
     * 应用场景：表示成员在消费组协议中的当前阶段，例如正在加入、稳定、离开等。
     * 实现细节：通过不同的操作（如心跳响应、用户调用）在不同的 {@link MemberState} 之间转换。
     * 设计考虑：状态机模型有助于清晰地管理成员的复杂生命周期。
     */
    protected MemberState state; // 字段：成员状态，表示成员在消费组中的当前状态（例如，加入中、稳定、已离开等）。

    /**
     * 成员从服务器收到并成功处理的分配，以及其本地 epoch。
     *
     * 当我们不在组中，或者尚未协调任何分配时，此值等于 LocalAssignment.NONE。
     * 应用场景：存储成员当前持有的、已经过协调和确认的分区分配。
     * 实现细节：在成功处理服务器下发的分配后更新。包含分区信息和对应的 epoch。
     * 设计考虑：确保成员只处理其当前 epoch 下的有效分配。
     */
    private LocalAssignment currentAssignment; // 字段：当前分配，表示成员当前持有的、已成功处理的分区分配。

    /**
     * 订阅状态对象，持有成员为其订阅的主题的当前分配。
     * 应用场景：管理消费者的订阅信息（订阅的主题、模式或特定分区）以及这些订阅对应的当前分区分配状态。
     * 实现细节：在构造时传入，是 final 类型。它内部维护了分配给此消费者的具体分区和偏移量信息。
     * 设计考虑：将订阅和分配状态封装在一个独立的对象中，便于管理和传递。
     */
    protected final SubscriptionState subscriptions; // 字段：订阅状态，管理消费者订阅的主题和分区。

    /**
     * 元数据，允许我们创建 {@link ConsumerRebalanceListener} 所需的分区。
     * 应用场景：在执行重平衡监听器回调时，需要访问集群的元数据（如主题信息）来构建 TopicPartition 对象。
     * 实现细节：在构造时传入，是 final 类型。用于查询主题和分区的最新信息。
     * 设计考虑：提供对集群元数据的访问，以支持重平衡回调的正确执行。
     */
    private final ConsumerMetadata metadata; // 字段：消费者元数据，用于获取集群和主题的元数据信息。

    /**
     * 日志记录器。
     * 应用场景：用于记录成员资格管理器在运行过程中的重要事件、警告和错误。
     * 实现细节：在构造时初始化，是 final 类型。
     * 设计考虑：提供标准的日志记录能力，方便问题排查和系统监控。
     */
    protected final Logger log; // 字段：日志记录器。

    /**
     * 已分配主题 ID 和名称的本地缓存。当在目标分配中收到主题时，
     * 我们在元数据缓存中发现它们的主题名称并将其添加此处；当主题不再订阅时，则将其删除。
     * 此缓存的目的是避免在当前分配的主题位于目标分配中（新分区已分配或已撤销），
     * 但此刻元数据缓存中不存在该主题时发起元数据请求。
     * 当订阅发生更改 ({@link #transitionToJoining()})、成员失败 ({@link #transitionToFatal()}) 
     * 或离开组 ({@link #leaveGroup()}/{@link #leaveGroupOnClose()}) 时，缓存将被清除。
     * 应用场景：优化元数据请求，当处理分配时，如果主题名称已知，则无需再次查询元数据。
     * 实现细节：使用一个 Map 存储 Topic ID 到 Topic Name 的映射。在特定事件（如订阅变更、成员离开）时清空。
     * 设计考虑：减少不必要的元数据请求，提高处理效率，特别是在元数据可能暂时不可用的情况下。
     */
    private final Map<Uuid, String> assignedTopicNamesCache; // 字段：已分配主题名称缓存，用于缓存已分配主题的ID和名称映射。

    /**
     * 在上一个目标分配中收到的主题 ID 和分区，以及其本地 epoch。
     *
     * 每次收到新的分配时，都会重新分配此成员变量。
     * 当我们不在组中时，它等于 LocalAssignment.NONE。
     * 应用场景：存储从服务器接收到的最新目标分配，等待成员进行协调处理。
     * 实现细节：在收到心跳响应并包含新分配时更新。包含分区信息和对应的 epoch。
     * 设计考虑：作为当前分配（currentAssignment）的前置状态，表示服务器期望成员拥有的分配。
     */
    private LocalAssignment currentTargetAssignment; // 字段：当前目标分配，表示从服务器接收到的最新目标分区分配。

    /**
     * 如果正在为 assignmentReadyToReconcile 运行协调（触发提交、回调）。
     * 如果在收到心跳响应或元数据更新后触发了 {@link #maybeReconcile()}，则此值为 true。
     * 应用场景：标记当前是否正在进行分区分配的协调过程（包括提交偏移量、调用重平衡监听器等）。
     * 实现细节：布尔标志位，在开始协调时设置为 true，协调完成或中断时设置为 false。
     * 设计考虑：防止并发的协调操作，确保协调过程的原子性。
     */
    private boolean reconciliationInProgress; // 字段：协调是否正在进行中，标记当前是否正在处理分区分配的协调逻辑。

    /**
     * 如果协调正在进行中，并且成员自协调开始以来重新加入了组，则为 True。
     * 用于知晓正在进行的协调应被中断且不应被应用。
     * 应用场景：处理在协调过程中成员被踢出并重新加入组的情况，此时旧的协调操作应被取消。
     * 实现细节：布尔标志位，当成员在协调过程中重新加入组时设置为 true。
     * 设计考虑：确保只应用最新的、与当前成员资格一致的协调结果。
     */
    private boolean rejoinedWhileReconciliationInProgress; // 字段：协调过程中是否重新加入组，用于中断过时的协调操作。

    /**
     * 如果成员在调用 {@link #leaveGroup()} 或 {@link #leaveGroupOnClose()} 后当前正在离开组，
     * 这将持有一个 future，该 future 将在正在进行的离开操作完成时完成（回调已执行且已发送离开心跳请求）。
     * 如果成员没有离开，则此项为空。
     * 应用场景：用于跟踪异步离开组操作的状态，确保在离开操作完成前不会执行冲突操作。
     * 实现细节：使用 {@link Optional} 包装 {@link CompletableFuture}，表示离开操作可能正在进行也可能没有。
     * 设计考虑：异步操作需要一种机制来通知其完成，CompletableFuture 适合此场景。
     */
    private Optional<CompletableFuture<Void>> leaveGroupInProgress = Optional.empty(); // 字段：离开组操作的 Future，用于跟踪异步离开组操作的完成状态。

    /**
     * 已注册的监听器，当成员 epoch 更新时（从代理接收到有效值，或由于成员离开组、被隔离或失败而清除值），
     * 这些监听器将收到通知。
     * 应用场景：允许其他组件在成员状态（特别是 epoch）发生变化时得到通知并做出相应处理。
     * 实现细节：使用 {@link List} 存储 {@link MemberStateListener} 接口的实现。
     * 设计考虑：提供一个回调机制，解耦成员状态变化通知的逻辑。
     */
    private final List<MemberStateListener> stateUpdatesListeners; // 字段：状态更新监听器列表，用于在成员 epoch 更新时通知相关方。

    /**
     * 当一个过时成员（因轮询计时器过期而离开组）完成释放其分配后，此 Future 将完成。
     * 用于确保成员在计时器重置后重新加入组时，仅在其完成释放分配后才进行。
     * 应用场景：处理因长时间未轮询而被标记为过期的成员，确保其在重新加入前正确释放资源。
     * 实现细节：使用 {@link CompletableFuture} 来跟踪异步的分配释放操作。
     * 设计考虑：确保过时成员在重新激活前完成必要的清理工作，避免状态不一致。
     */
    private CompletableFuture<Void> staleMemberAssignmentRelease; // 字段：过时成员分配释放的 Future，用于跟踪因 poll 超时而离开的成员释放其分配的操作。

    /**
     * 衡量成功的重平衡延迟和失败的重平衡次数。
     * 应用场景：收集和报告与消费者重平衡相关的性能指标。
     * 实现细节：依赖 {@link RebalanceMetricsManager} 接口的实现来记录和管理度量数据。
     * 设计考虑：将度量收集逻辑封装到专门的管理器中，便于扩展和维护。
     */
    private final RebalanceMetricsManager metricsManager; // 字段：重平衡度量管理器，用于记录重平衡相关的指标，如延迟和失败次数。

    /**
     * 时间接口，用于获取当前时间。
     * 应用场景：在需要记录时间戳或进行超时判断等操作时，提供统一的时间源。
     * 实现细节：依赖 {@link Time} 接口的实现，通常在测试中会使用可控制的 MockTime。
     * 设计考虑：解耦对系统时间的直接依赖，方便测试和模拟不同时间场景。
     */
    private final Time time; // 字段：时间工具，用于获取当前时间戳等。

    /**
     * 用于跟踪订阅是否已更新的 AtomicBoolean。
     * 如果为 true 且订阅状态为 UNSUBSCRIBED，则下一次调用 {@link #onConsumerPoll()} 时会将成员状态更改为 JOINING。
     * 应用场景：标记消费者的订阅信息是否发生了变化，以便在合适的时机（如下次 poll）触发加入组的逻辑。
     * 实现细节：使用 {@link AtomicBoolean} 确保线程安全的更新和读取。
     * 设计考虑：避免在订阅更新后立即触发加入组，而是延迟到下一次 poll 操作，以减少不必要的网络交互。
     */
    private final AtomicBoolean subscriptionUpdated = new AtomicBoolean(false); // 字段：订阅更新标志，原子布尔型，用于标记订阅信息是否已更新。

    /**
     * 如果轮询计时器已过期，则为 True，通过调用 {@link #transitionToSendingLeaveGroup(boolean)} 并将 dueToExpiredPollTimer 参数设置为 true 来发出信号。
     * 这将用于确定成员在离开组后应转换为 STALE 状态，以释放其分配并等待计时器重置。
     * 应用场景：标记成员是否因为长时间未调用 poll 方法而导致其会话被认为过期。
     * 实现细节：布尔标志位，在检测到 poll 超时时设置。
     * 设计考虑：用于区分正常的离开组和因 poll 超时导致的离开，后者需要进入 STALE 状态进行特殊处理。
     */
    private boolean isPollTimerExpired; // 字段：轮询计时器是否过期标志。

    /**
     * 抽象成员资格管理器的构造函数。
     * 应用场景：初始化成员资格管理器所需的核心组件和配置。
     * 实现细节：保存传入的 groupId、subscriptions、metadata、log、time 和 metricsManager。
     *           初始化成员状态为 UNSUBSCRIBED，分配相关的缓存和当前分配为初始状态。
     * 设计考虑：确保所有必要的依赖项在对象创建时都已提供和初始化。
     *
     * @param groupId 消费组 ID。
     * @param subscriptions 订阅状态对象，管理消费者的订阅信息。
     * @param metadata 消费者元数据，用于获取集群和主题信息。
     * @param log 日志记录器。
     * @param time 时间工具。
     * @param metricsManager 重平衡度量管理器。
     */
    AbstractMembershipManager(String groupId, // 构造函数参数：消费组ID
                              SubscriptionState subscriptions, // 构造函数参数：订阅状态
                              ConsumerMetadata metadata, // 构造函数参数：消费者元数据
                              Logger log, // 构造函数参数：日志记录器
                              Time time, // 构造函数参数：时间工具
                              RebalanceMetricsManager metricsManager) { // 构造函数参数：重平衡度量管理器
        this.groupId = groupId; // 初始化消费组ID
        this.state = MemberState.UNSUBSCRIBED; // 初始化成员状态为“未订阅”
        this.subscriptions = subscriptions; // 初始化订阅状态
        this.metadata = metadata; // 初始化消费者元数据
        this.assignedTopicNamesCache = new HashMap<>(); // 初始化已分配主题名称缓存为空的HashMap
        this.currentTargetAssignment = LocalAssignment.NONE; // 初始化当前目标分配为“无”
        this.currentAssignment = LocalAssignment.NONE; // 初始化当前分配为“无”
        this.log = log; // 初始化日志记录器
        this.stateUpdatesListeners = new ArrayList<>(); // 初始化状态更新监听器列表为空的ArrayList
        this.time = time; // 初始化时间工具
        this.metricsManager = metricsManager; // 初始化重平衡度量管理器
    }

    /**
     * 更新成员状态，仅当转换有效时才将其设置为 nextState。
     * 应用场景：管理成员在消费组协议中的状态流转，确保状态转换的合法性。
     * 实现细节：检查当前状态到目标状态的转换是否在 {@link MemberState} 中定义为有效转换。
     *           如果转换有效，则更新状态，并根据状态变化记录重平衡开始或结束的度量。
     * 设计考虑：通过状态机模型来管理成员状态，使状态转换逻辑更清晰和可控。
     *
     * @param nextState 要转换到的下一个成员状态。
     * @throws IllegalStateException 如果从成员的当前 {@link #state} 转换到 nextState 是无效的（即未在 {@link MemberState} 中定义）。
     */
    protected void transitionTo(MemberState nextState) { // 方法：转换到指定成员状态
        // 检查当前状态是否与目标状态不同，并且目标状态的前置有效状态列表是否不包含当前状态
        if (!state.equals(nextState) && !nextState.getPreviousValidStates().contains(state)) {
            // 如果转换无效，则抛出IllegalStateException异常
            throw new IllegalStateException(String.format("无效的状态转换，从 %s 到 %s",
                    state, nextState));
        }

        // 如果当前状态转换表示重平衡完成
        if (isCompletingRebalance(state, nextState)) {
            // 记录重平衡结束的度量信息
            metricsManager.recordRebalanceEnded(time.milliseconds());
        }
        // 如果当前状态转换表示重平衡开始
        if (isStartingRebalance(state, nextState)) {
            // 记录重平衡开始的度量信息
            metricsManager.recordRebalanceStarted(time.milliseconds());
        }

        // 记录成员状态转换的日志信息
        log.info("成员 {} (epoch {}) 从 {} 转换到 {}.", memberId, memberEpoch, state, nextState);
        // 更新成员的当前状态为目标状态
        this.state = nextState;
    }

    /**
     * 判断状态转换是否表示重平衡操作的完成。
     * 应用场景：在状态转换时，用于确定是否需要记录重平衡结束的度量指标。
     * 实现细节：当当前状态是 RECONCILING，并且下一个状态是 STABLE 或 ACKNOWLEDGING 时，认为重平衡已完成。
     * 设计考虑：清晰地定义了重平衡完成的条件，便于逻辑判断。
     *
     * @param currentState 当前成员状态。
     * @param nextState    下一个成员状态。
     * @return 如果状态转换表示重平衡完成，则返回 true；否则返回 false。
     */
    private static boolean isCompletingRebalance(MemberState currentState, MemberState nextState) { // 方法：判断是否正在完成重平衡
        // 如果当前状态是RECONCILING（协调中）
        // 并且下一个状态是STABLE（稳定）或ACKNOWLEDGING（确认中）
        return currentState == MemberState.RECONCILING &&
            (nextState == MemberState.STABLE || nextState == MemberState.ACKNOWLEDGING);
    }

    /**
     * 判断状态转换是否表示重平衡操作的开始。
     * 应用场景：在状态转换时，用于确定是否需要记录重平衡开始的度量指标。
     * 实现细节：当当前状态不是 RECONCILING，并且下一个状态是 RECONCILING 时，认为重平衡已开始。
     * 设计考虑：清晰地定义了重平衡开始的条件，便于逻辑判断。
     *
     * @param currentState 当前成员状态。
     * @param nextState    下一个成员状态。
     * @return 如果状态转换表示重平衡开始，则返回 true；否则返回 false。
     */
    private static boolean isStartingRebalance(MemberState currentState, MemberState nextState) { // 方法：判断是否正在开始重平衡
        // 如果当前状态不是RECONCILING（协调中）
        // 并且下一个状态是RECONCILING（协调中）
        return currentState != MemberState.RECONCILING && nextState == MemberState.RECONCILING;
    }

    /**
     * 获取成员所属（或想要加入）的组的组 ID。
     * 应用场景：提供给外部调用者以获取当前成员管理器关联的消费组 ID。
     * 实现细节：直接返回构造时传入的 groupId 字段。
     * 设计考虑：提供一个公开的访问方法来获取组 ID。
     *
     * @return 消费组 ID。
     */
    public String groupId() { // 方法：获取消费组ID
        // 返回成员所属的消费组ID
        return groupId;
    }

    /**
     * 获取在启动时生成并在进程的整个生命周期内保持不变的成员 ID。
     * 应用场景：提供给外部调用者以获取当前成员的唯一标识符。
     * 实现细节：直接返回在对象实例化时生成的 memberId 字段。
     * 设计考虑：提供一个公开的访问方法来获取成员 ID。
     *
     * @return 成员 ID。
     */
    public String memberId() { // 方法：获取成员ID
        // 返回成员的ID
        return memberId;
    }

    /**
     * 获取由服务器维护的成员的当前 epoch。
     * 应用场景：提供给外部调用者以获取成员当前的 epoch 值，用于判断成员资格的有效性。
     * 实现细节：直接返回 memberEpoch 字段。
     * 设计考虑：提供一个公开的访问方法来获取成员 epoch。
     *
     * @return 当前成员 epoch。
     */
    public int memberEpoch() { // 方法：获取成员epoch
        // 返回成员的当前epoch
        return memberEpoch;
    }

    /**
     * 根据成功的心跳响应更新成员信息并转换成员状态。
     * 应用场景：这是处理来自 Broker 的成功心跳响应的核心逻辑，子类需要根据具体的协议来实现。
     * 实现细节：这是一个抽象方法，具体的实现由子类（如 ConsumerMembershipManager）提供，
     *           通常会解析响应中的成员 ID、epoch、分配等信息，并据此更新本地状态和转换成员状态。
     * 设计考虑：将特定于协议的心跳响应处理逻辑延迟到子类实现，保持抽象类的通用性。
     *
     * @param response 心跳响应，用于提取成员信息和错误。
     */
    public abstract void onHeartbeatSuccess(R response); // 抽象方法：处理成功的心跳响应

    /**
     * 通知成员收到了错误的心跳响应。
     * 应用场景：处理心跳请求失败的情况，例如网络错误、Broker 返回错误码等。
     * 实现细节：如果错误是不可重试的，则记录重平衡失败的度量。
     *           如果当前状态是 UNSUBSCRIBED 并且正在进行离开组的操作，则尝试完成离开操作，因为离开组请求只发送一次，无论响应如何都应完成操作。
     * 设计考虑：区分可重试和不可重试的错误，并对特定的状态（如正在离开组）进行特殊处理。
     *
     * @param retriable 如果请求因可重试错误而失败，则为 True。
     */
    public void onHeartbeatFailure(boolean retriable) { // 方法：处理心跳失败
        // 如果错误是不可重试的
        if (!retriable) {
            // 尝试记录重平衡失败的度量信息
            metricsManager.maybeRecordRebalanceFailed();
        }
        // 离开组请求只发送一次（不重试），因此一旦请求完成，无论响应如何，我们都应该完成离开操作。
        // 如果当前状态是UNSUBSCRIBED（未订阅）并且可能正在进行离开组的操作
        if (state == MemberState.UNSUBSCRIBED && maybeCompleteLeaveInProgress()) {
            // 记录警告日志，表明成员收到了离开组心跳的失败响应，但仍完成了离开操作
            log.warn("成员 {} (epoch {}) 收到了离开组心跳的失败响应，并完成了离开操作。", memberId, memberEpoch);
        }
    }

    /**
     * 完成正在进行的离开操作（如果存在）。这预计在成员收到离开心跳的响应时用于完成正在进行的离开操作。
     * 应用场景：当成员主动离开消费组，并收到了Broker对离开请求的确认响应后，调用此方法来清理离开操作相关的状态。
     * 实现细节：检查是否存在正在进行的离开操作（`leaveGroupInProgress` 是否有值），如果存在，则调用其 `complete(null)` 方法来标记完成，
     * 并将 `leaveGroupInProgress` 重置为空的 `Optional`。
     * 设计考虑：使用 `Optional` 和 `CompletableFuture` 来管理异步的离开操作，确保离开流程的正确完成和状态清理。
     * @return 如果成功完成了正在进行的离开操作，则返回 true；否则返回 false。
     */
    protected boolean maybeCompleteLeaveInProgress() {
        // 检查 leaveGroupInProgress 是否包含一个 CompletableFuture，表示有一个正在进行的离开操作
        if (leaveGroupInProgress.isPresent()) {
            // 如果存在，获取该 CompletableFuture 并调用 complete(null) 来标记它已成功完成
            leaveGroupInProgress.get().complete(null);
            // 将 leaveGroupInProgress 重置为一个空的 Optional，表示没有正在进行的离开操作了
            leaveGroupInProgress = Optional.empty();
            // 返回 true，表示成功完成了离开操作
            return true;
        }
        // 如果没有正在进行的离开操作，则返回 false
        return false;
    }

    /**
     * 检查消费者是否不属于该组的成员。
     * 应用场景：在执行某些操作前，需要判断消费者当前是否是组的有效成员。例如，某些操作只应在成员不活跃或无效时执行。
     * 实现细节：通过检查成员的当前状态 (`state`) 是否为 `UNSUBSCRIBED`（未订阅）、`FENCED`（被隔离）、`FATAL`（致命错误）或 `STALE`（过时）中的任意一种来判断。
     * 设计考虑：定义了一组明确的状态来表示成员不再是组的活跃部分，方便进行逻辑判断。
     * @return 如果消费者不属于该组的成员，则返回 true；否则返回 false。
     */
    protected boolean isNotInGroup() {
        // 检查成员的当前状态是否为以下几种非活跃或无效状态之一：
        // MemberState.UNSUBSCRIBED: 成员已取消订阅，不参与消费组。
        // MemberState.FENCED: 成员被隔离，通常是因为其 epoch 过期，不能再参与消费组活动。
        // MemberState.FATAL: 成员遇到了致命错误，无法继续正常工作。
        // MemberState.STALE: 成员状态过时，可能已被组协调器移除。
        return state == MemberState.UNSUBSCRIBED ||
            state == MemberState.FENCED ||
            state == MemberState.FATAL ||
            state == MemberState.STALE;
    }

    /**
     * 如果收到的分配与成员当前的分配不同，则处理该分配。
     * 如果收到新的分配，这将确保在下一次调用 `poll` 时尝试进行协调。
     * 如果当前正在进行另一个协调，则在该协调之后的第一次 `poll` 将触发新的协调。
     * 应用场景：当消费者从Broker（通常是组协调器）接收到新的分区分配时，调用此方法来更新其目标分配并根据需要转换状态以进行协调。
     * 实现细节：
     * 1. 调用 `replaceTargetAssignmentWithNewAssignment` 方法，用新的分配更新 `currentTargetAssignment`。
     * 2. 检查目标分配是否已协调（`targetAssignmentReconciled()`）。
     *    - 如果未协调（即新分配与当前分配不同），则将成员状态转换为 `RECONCILING`，准备在后续的 `poll` 循环中执行协调逻辑（如调用 `onPartitionsAssigned` 和 `onPartitionsRevoked` 监听器）。
     *    - 如果已协调（即新分配与当前分配相同），则记录调试信息，并检查当前状态是否为 `RECONCILING` 或 `JOINING`。如果是，则将状态转换回 `STABLE`，因为无需进一步协调。
     * 设计考虑：此方法是处理Broker下发分配的核心入口。它确保了只有在分配发生变化时才触发协调，并正确管理成员状态的转换。
     * @param assignment 从Broker收到的分配信息，是一个映射，键是主题的UUID，值是该主题下分配给此成员的分区号集合。
     */
    protected void processAssignmentReceived(Map<Uuid, SortedSet<Integer>> assignment) {
        // 使用从Broker接收到的新分配替换当前的目标分配
        replaceTargetAssignmentWithNewAssignment(assignment);
        // 检查目标分配是否已经与当前分配一致（即是否已协调）
        if (!targetAssignmentReconciled()) {
            // 如果目标分配与当前分配不同（即未协调）：
            // 当从Broker收到与当前分配不同的新目标分配时，将成员转换为 RECONCILING 状态。
            // 注意，由于缺少元数据，协调可能不会立即触发。
            transitionTo(MemberState.RECONCILING);
        } else {
            // 如果目标分配与当前分配相同（即已协调）：
            // 收到相同的分配，无需协调。
            log.debug("Target assignment {} received from the broker is equals to the member " +
                    "current assignment {}. Nothing to reconcile.",
                currentTargetAssignment, currentAssignment); // 记录调试信息，表明收到的目标分配与当前分配相同
            // 确保如果成员之前处于 RECONCILING 状态（例如，成员正在协调刚刚被Broker删除的未解析分配），
            // 或者处于 JOINING 状态（例如，正在加入的成员收到了空分配），则将其转换回 STABLE 状态。
            if (state == MemberState.RECONCILING || state == MemberState.JOINING) {
                // 将成员状态转换为 STABLE
                transitionTo(MemberState.STABLE);
            }
        }
    }

    /**
     * 使用新的目标分配覆盖当前的目标分配。
     * 应用场景：当从Broker接收到新的分区分配指令时，调用此方法来更新成员内部维护的目标分配信息。
     * 实现细节：
     * 1. 调用 `currentTargetAssignment`（一个 `LocalAssignment` 对象）的 `updateWith` 方法，传入从Broker收到的新分配 `assignment`。
     * 2. `updateWith` 方法会比较新分配与当前目标分配，如果不同，则创建一个新的 `LocalAssignment` 对象并返回（包装在 `Optional` 中）。
     * 3. 如果 `updateWith` 返回了一个更新后的分配（即 `Optional` 非空），则记录一条调试日志，
     *    并用这个 `updatedAssignment` 更新成员的 `currentTargetAssignment` 字段。
     * 设计考虑：`LocalAssignment` 内部可能封装了分配的版本信息（如epoch），`updateWith` 的逻辑确保了只有在分配确实发生变化时才进行更新，
     * 并可能更新相关的版本信息。这种方式有助于跟踪分配的变更历史和确保一致性。
     * @param assignment 从Broker收到的目标分配，是一个映射，键是主题的UUID，值是该主题下分配给此成员的分区号集合。
     */
    private void replaceTargetAssignmentWithNewAssignment(Map<Uuid, SortedSet<Integer>> assignment) {
        // 调用 currentTargetAssignment 的 updateWith 方法，尝试使用新的 assignment 更新它
        // updateWith 方法会比较新的分配和当前的 currentTargetAssignment，如果不同，则返回包含更新后分配的 Optional
        currentTargetAssignment.updateWith(assignment).ifPresent(updatedAssignment -> {
            // 如果 currentTargetAssignment 被成功更新（即 ifPresent 的 lambda 表达式被执行）：
            // 记录调试信息，表明目标分配已从旧值更新为新值，并提示成员将在下一次 poll 时协调此新分配
            log.debug("Target assignment updated from {} to {}. Member will reconcile it on the next poll.",
                currentTargetAssignment, updatedAssignment);
            // 将成员的 currentTargetAssignment 字段更新为新的 updatedAssignment
            currentTargetAssignment = updatedAssignment;
        });
    }

    /**
     * 将成员转换到 FENCED 状态，在该状态下，成员将通过调用 onPartitionsLost 回调来释放分配，
     * 并且当回调完成时，它将转换到 {@link MemberState#JOINING} 状态以重新加入组。
     * 这预计在心跳返回 FENCED_MEMBER_EPOCH 或 UNKNOWN_MEMBER_ID 错误时被调用。
     * 应用场景：当Broker通过心跳响应告知成员其epoch已过时（FENCED_MEMBER_EPOCH）或成员ID未知（UNKNOWN_MEMBER_ID）时，
     * 成员需要进入FENCED状态，释放当前分区，并尝试重新加入组以获取新的epoch和分配。
     * 实现细节：
     * 1. 处理特殊状态：如果成员已处于 `PREPARE_LEAVING` 或 `LEAVING` 状态，则记录日志，转换为 `UNSUBSCRIBED` 并完成离开操作，不再尝试重新加入。
     * 2. 如果成员已处于 `UNSUBSCRIBED` 状态，则记录日志并直接返回，因为成员已离开组。
     * 3. 对于其他状态，将成员状态转换为 `FENCED`。
     * 4. 调用 `resetEpoch()` 重置成员的epoch。
     * 5. 记录成员转换到 `FENCED` 状态的日志。
     * 6. 调用 `signalPartitionsLost` 异步触发 `onPartitionsLost` 回调，传入当前已分配的分区。
     * 7. 在 `onPartitionsLost` 回调完成后（无论成功或失败）：
     *    a. 如果回调出错，记录错误日志。
     *    b. 调用 `clearAssignment()` 清除当前分配。
     *    c. 如果此时成员状态仍然是 `FENCED`，则调用 `transitionToJoining()` 使成员尝试重新加入组。
     *    d. 如果状态已改变（例如，在回调执行期间发生了其他事件导致状态变化），则记录日志，成员不再重新加入。
     * 设计考虑：FENCED状态是一个关键的错误恢复机制。它确保了当成员与Broker状态不一致时，能够安全地释放资源并尝试恢复。异步回调处理允许非阻塞地执行用户逻辑。
     */
    public void transitionToFenced() {
        // 如果成员当前处于 PREPARE_LEAVING 状态
        if (state == MemberState.PREPARE_LEAVING) {
            // 记录日志：成员被隔离，但已在准备离开组，因此将停止发送心跳，不会尝试发送离开请求或重新加入
            log.info("Member {} with epoch {} got fenced but it is already preparing to leave " +
                    "the group, so it will stop sending heartbeat and won't attempt to send the " +
                    "leave request or rejoin.", memberId, memberEpoch);
            // 短暂转换到 LEAVING 状态以确保应用所有必要的操作，即使不需要发送离开组心跳
            // （例如，清除 epoch 并通知 epoch 监听器）。
            // 然后转换到 UNSUBSCRIBED，确保成员（从Broker的角度看已不再是组的一部分）
            // 在完成正在进行的离开操作时停止发送心跳。
            transitionToSendingLeaveGroup(false); // 转换到发送离开组请求的状态（但不实际发送请求）
            transitionTo(MemberState.UNSUBSCRIBED); // 转换到未订阅状态
            maybeCompleteLeaveInProgress(); // 尝试完成正在进行的离开操作
            return; // 方法结束
        }

        // 如果成员当前处于 LEAVING 状态
        if (state == MemberState.LEAVING) {
            // 记录日志：成员在发送离开组心跳之前被隔离。它将不会发送离开请求，也不会尝试重新加入。
            log.debug("Member {} with epoch {} got fenced before sending leave group heartbeat. " +
                    "It will not send the leave request and won't attempt to rejoin.", memberId, memberEpoch);
            transitionTo(MemberState.UNSUBSCRIBED); // 转换到未订阅状态
            maybeCompleteLeaveInProgress(); // 尝试完成正在进行的离开操作
            return; // 方法结束
        }
        // 如果成员当前处于 UNSUBSCRIBED 状态
        if (state == MemberState.UNSUBSCRIBED) {
            // 记录日志：成员被隔离，但它已经离开了组，所以不会尝试重新加入。
            log.debug("Member {} with epoch {} got fenced but it already left the group, so it " +
                    "won't attempt to rejoin.", memberId, memberEpoch);
            return; // 方法结束
        }
        // 将成员状态转换为 FENCED
        transitionTo(MemberState.FENCED);
        // 重置成员的 epoch
        resetEpoch();
        // 记录日志：成员已转换到 FENCED 状态，它将释放其分配并重新加入组。
        log.debug("Member {} with epoch {} transitioned to {} state. It will release its " +
                "assignment and rejoin the group.", memberId, memberEpoch, MemberState.FENCED);

        // 释放分配
        // 调用 signalPartitionsLost 方法，通知监听器分区已丢失，并获取一个 CompletableFuture 以跟踪回调的完成
        CompletableFuture<Void> callbackResult = signalPartitionsLost(subscriptions.assignedPartitions());
        // 当 onPartitionsLost 回调完成时执行以下逻辑
        callbackResult.whenComplete((result, error) -> {
            // 如果回调执行过程中发生错误
            if (error != null) {
                // 记录错误日志：在成员被隔离后释放分配时，onPartitionsLost 回调调用失败。成员仍将尝试重新加入组。
                log.error("onPartitionsLost callback invocation failed while releasing assignment" +
                        " after member got fenced. Member will rejoin the group anyways.", error);
            }
            // 清除当前分配信息
            clearAssignment();
            // 检查回调完成时成员的状态是否仍然是 FENCED
            if (state == MemberState.FENCED) {
                // 如果状态仍为 FENCED，则转换到 JOINING 状态以尝试重新加入组
                transitionToJoining();
            } else {
                // 如果状态已改变，记录日志：被隔离成员的 onPartitionsLost 回调已完成，但状态已更改，因此成员不会重新加入组
                log.debug("Fenced member onPartitionsLost callback completed but the state has " +
                    "already changed to {}, so the member won't rejoin the group", state);
            }
        });
    }

    /**
     * 将成员转换到 FATAL 状态并根据需要更新成员信息。这在发生不可恢复的错误时调用
     * （例如，当心跳返回不可重试的错误时）。
     * 应用场景：当消费者遇到无法恢复的严重错误（例如，Broker返回了致命错误码，或者内部发生严重异常），
     * 需要将成员置于FATAL状态，停止所有活动，并释放资源。
     * 实现细节：
     * 1. 保存成员转换前的状态 `previousState`。
     * 2. 将成员状态转换为 `FATAL`。
     * 3. 记录错误日志，表明成员已进入FATAL状态。
     * 4. 调用 `notifyEpochChange(Optional.empty())` 通知监听器epoch已失效（因为成员已无法工作）。
     * 5. 处理特殊的前置状态：
     *    - 如果 `previousState` 是 `UNSUBSCRIBED`，则记录调试信息并返回，因为成员已离开组，无需触发 `onPartitionsLost`。
     *    - 如果 `previousState` 是 `LEAVING` 或 `PREPARE_LEAVING`，则记录信息，表明成员在离开过程中遇到致命错误，将放弃离开操作并保持FATAL状态，然后尝试完成离开操作的清理。
     * 6. 对于其他前置状态，表示成员之前是活跃的，需要释放其分区分配：
     *    a. 调用 `signalPartitionsLost` 异步触发 `onPartitionsLost` 回调，传入当前已分配的分区。
     *    b. 在 `onPartitionsLost` 回调完成后（无论成功或失败）：
     *       i. 如果回调出错，记录错误日志。
     *       ii. 调用 `clearAssignment()` 清除当前分配。
     * 设计考虑：FATAL状态表示成员已无法正常工作，必须停止。通过 `onPartitionsLost` 确保用户有机会清理与已分配分区相关的资源。异步回调处理允许非阻塞地执行用户逻辑。
     */
    public void transitionToFatal() {
        // 保存成员进入 FATAL 状态之前的状态
        MemberState previousState = state;
        // 将成员状态转换为 FATAL
        transitionTo(MemberState.FATAL);
        // 记录错误日志，表明成员已转换到致命状态
        log.error("Member {} with epoch {} transitioned to fatal state", memberId, memberEpoch);
        // 通知 epoch 监听器 epoch 已更改（变为空，表示无效）
        notifyEpochChange(Optional.empty());

        // 如果成员在进入 FATAL 状态之前已经是 UNSUBSCRIBED 状态
        if (previousState == MemberState.UNSUBSCRIBED) {
            // 记录调试信息：成员从Broker收到致命错误，但它已经离开了组，所以 onPartitionsLost 回调不会被触发。
            log.debug("Member {} with epoch {} got fatal error from the broker but it already " +
                    "left the group, so onPartitionsLost callback won't be triggered.", memberId, memberEpoch);
            // 直接返回，无需进一步处理
            return;
        }

        // 如果成员在进入 FATAL 状态之前处于 LEAVING 或 PREPARE_LEAVING 状态
        if (previousState == MemberState.LEAVING || previousState == MemberState.PREPARE_LEAVING) {
            // 记录信息：成员在离开组的过程中（状态为 previousState）从Broker收到致命错误。
            // 它将放弃正在进行的离开操作，并保持在致命状态。
            log.info("Member {} with epoch {} was leaving the group with state {} when it got a " +
                "fatal error from the broker. It will discard the ongoing leave and remain in " +
                "fatal state.", memberId, memberEpoch, previousState);
            // 尝试完成（或取消）正在进行的离开操作
            maybeCompleteLeaveInProgress();
            // 直接返回
            return;
        }

        // 对于其他之前的状态（例如 STABLE, JOINING, RECONCILING, FENCED），需要释放分配
        // 调用 signalPartitionsLost 方法，通知监听器分区已丢失，并获取一个 CompletableFuture 以跟踪回调的完成
        CompletableFuture<Void> callbackResult = signalPartitionsLost(subscriptions.assignedPartitions());
        // 当 onPartitionsLost 回调完成时执行以下逻辑
        callbackResult.whenComplete((result, error) -> {
            // 如果回调执行过程中发生错误
            if (error != null) {
                // 记录错误日志：在成员因致命错误失败后释放分配时，onPartitionsLost 回调调用失败。
                log.error("onPartitionsLost callback invocation failed while releasing assignment" +
                        "after member failed with fatal error.", error);
            }
            // 清除当前分配信息
            clearAssignment();
        });
    }

    /**
     * 将 {@link #subscriptionUpdated} 设置为 true，表示订阅已更新。
     * 下一个 {@link #onConsumerPoll()} 将使用更新后的订阅加入组（如果成员尚未加入组）。
     * 如果成员已经是组的一部分，这只会确保更新后的订阅包含在下一个心跳请求中。
     * <p/>
     * 注意：订阅的主题列表取自共享订阅状态。
     * @implSpec 此方法用于标记订阅状态已发生变化，通常在用户调用 `subscribe()` API 后触发。
     *           通过原子操作 `compareAndSet` 确保线程安全，避免竞态条件。
     */
    public void onSubscriptionUpdated() {
        // 原子地将 subscriptionUpdated 从 false 更新为 true
        // 如果已经是 true，则不执行任何操作
        // 这确保了即使多次调用，也只会在第一次调用时将状态标记为已更新
        subscriptionUpdated.compareAndSet(false, true);
    }

    /**
     * 如果成员尚未加入组，则加入组。此函数将 {@link #transitionToJoining} 从 {@link #onSubscriptionUpdated} 中分离出来，
     * 以满足“重平衡只会在对 {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(Duration)} 的活动调用期间发生”的要求。
     * @implSpec 此方法在消费者轮询时被调用，用于处理订阅更新后的加入组逻辑。
     *           它检查 `subscriptionUpdated` 标志位，并在成员处于 `UNSUBSCRIBED` 状态时转换到加入组的状态。
     *           `compareAndSet(true, false)` 确保 `transitionToJoining()` 只被调用一次，即使 `onConsumerPoll()` 被多次调用。
     */
    public void onConsumerPoll() {
        // 检查订阅是否已更新 (subscriptionUpdated 为 true) 并且当前成员状态为 UNSUBSCRIBED
        // compareAndSet(true, false) 会在检查后将 subscriptionUpdated 设置为 false，防止重复加入
        if (subscriptionUpdated.compareAndSet(true, false) && state == MemberState.UNSUBSCRIBED) {
            // 如果条件满足，则转换到加入组的状态
            transitionToJoining();
        }
    }

    /**
     * 清除成员订阅中的已分配分区、待处理的分配和元数据缓存。
     * @implSpec 此方法用于在成员离开组或发生错误时重置分配状态。
     *           它会清除订阅状态中的自动分配分区，并通知分配变更。
     *           同时，将当前本地分配设置为空，并清除待处理的分配和本地名称缓存。
     */
    private void clearAssignment() {
        // 检查订阅中是否有自动分配的分区
        if (subscriptions.hasAutoAssignedPartitions()) {
            // 如果有，则从订阅中分配一个空的分区集合，相当于清除自动分配
            subscriptions.assignFromSubscribed(Collections.emptySet());
            // 通知监听器分配已更改为空集合
            notifyAssignmentChange(Collections.emptySet());
        }
        // 将当前本地分配设置为 LocalAssignment.NONE，表示没有分配
        currentAssignment = LocalAssignment.NONE;
        // 清除所有待处理的分配和本地名称缓存
        clearPendingAssignmentsAndLocalNamesCache();
    }

    /**
     * 通过在成员订阅中设置已分配的分区来更新新的分配。
     * 这会将新添加的分区标记为待回调状态，以防止在回调运行时获取记录或更新其位置。
     *
     * @param assignedPartitions 完整分配，用于更新订阅状态
     * @param addedPartitions    新添加的分区
     * @implSpec 此方法在接收到新的分区分配后，但在执行用户回调（如 onPartitionsAssigned）之前调用。
     *           它将新的分配应用到订阅状态，并将新添加的分区标记为等待回调，
     *           这样可以确保在回调完成前，消费者不会开始处理这些新分区的数据。
     */
    private void updateSubscriptionAwaitingCallback(SortedSet<TopicIdPartition> assignedPartitions,
                                                    SortedSet<TopicPartition> addedPartitions) {
        // 将 TopicIdPartition 集合转换为 TopicPartition 集合
        Set<TopicPartition> assignedTopicPartitions = toTopicPartitionSet(assignedPartitions);
        // 更新订阅状态，将分配的分区设置为 assignedTopicPartitions，并将 addedPartitions 标记为等待回调
        subscriptions.assignFromSubscribedAwaitingCallback(assignedTopicPartitions, addedPartitions);
        // 通知监听器分配已更改为 assignedTopicPartitions
        notifyAssignmentChange(assignedTopicPartitions);
    }

    /**
     * 转换到 {@link MemberState#JOINING} 状态，表示成员将在下一个心跳请求时尝试加入组。
     * 这通常在用户调用 subscribe API 时，或者在成员被隔离 (fenced) 后想要重新加入时调用。
     * 此方法对测试可见。
     * @implSpec 此方法负责将成员状态设置为准备加入组。
     *           首先检查成员是否处于 FATAL 状态，如果是则不执行任何操作。
     *           如果当前正在进行协调 (reconciliation)，则标记在协调过程中发生了重新加入。
     *           然后重置成员的 epoch，转换到 JOINING 状态，并清除待处理的分配和本地名称缓存。
     */
    public void transitionToJoining() {
        // 如果当前成员状态是 FATAL (致命错误)
        if (state == MemberState.FATAL) {
            // 记录警告日志，说明由于成员处于 FATAL 状态，不执行加入组的操作
            log.warn("No action taken to join the group with the updated subscription because " +
                    "the member is in FATAL state");
            // 直接返回，不执行后续操作
            return;
        }
        // 如果当前正在进行分区协调 (reconciliation)
        if (reconciliationInProgress) {
            // 标记在协调过程中发生了重新加入组的尝试
            rejoinedWhileReconciliationInProgress = true;
        }
        // 重置成员的 epoch (纪元)，通常在重新加入组时需要
        resetEpoch();
        // 将成员状态转换为 JOINING
        transitionTo(MemberState.JOINING);
        // 清除所有待处理的分配和本地名称缓存，为新的分配做准备
        clearPendingAssignmentsAndLocalNamesCache();
    }

    /**
     * 转换到 {@link MemberState#PREPARE_LEAVING} 状态以释放分配。完成后，
     * 转换到 {@link MemberState#LEAVING} 状态以发送心跳请求并离开组。
     * 这通常在用户调用 {@link org.apache.kafka.clients.consumer.Consumer#close()} API 时调用。
     *
     * @return 当离开组的心跳已发送出去时将完成的 Future。
     * @implSpec 此方法是关闭消费者时离开组的入口点。它调用内部的 `leaveGroup` 方法，
     *           并且不执行用户定义的回调 (runCallbacks = false)，因为关闭操作通常意味着立即停止。
     */
    public CompletableFuture<Void> leaveGroupOnClose() {
        // 调用 leaveGroup 方法，参数 runCallbacks 设置为 false，表示在离开组时不执行 ConsumerRebalanceListener 回调
        return leaveGroup(false);
    }

    /**
     * 转换到 {@link MemberState#PREPARE_LEAVING} 状态以释放分配。完成后，
     * 转换到 {@link MemberState#LEAVING} 状态以发送心跳请求并离开组。
     * 这通常在用户调用 {@link org.apache.kafka.clients.consumer.Consumer#unsubscribe()} API 时调用。
     *
     * @return 当回调执行完成并且离开组的心跳已发送出去时将完成的 Future。
     * @implSpec 此方法是取消订阅时离开组的入口点。它调用内部的 `leaveGroup` 方法，
     *           并且执行用户定义的回调 (runCallbacks = true)，例如 `onPartitionsRevoked`。
     */
    public CompletableFuture<Void> leaveGroup() {
        // 调用 leaveGroup 方法，参数 runCallbacks 设置为 true，表示在离开组时执行 ConsumerRebalanceListener 回调
        return leaveGroup(true);
    }

    /**
     * 转换到 {@link MemberState#PREPARE_LEAVING} 状态以释放分配。完成后，
     * 转换到 {@link MemberState#LEAVING} 状态以发送心跳请求并离开组。
     * 这通常在用户调用取消订阅 API 或关闭消费者时调用。
     *
     * @param runCallbacks 如果为 {@code true}，则插入执行 {@link ConsumerRebalanceListener} 回调的步骤，
     *                     如果为 {@code false}，则跳过
     *
     * @return 当回调执行完成并且离开组的心跳已发送出去时将完成的 Future。
     * @implSpec 这是处理离开组的核心逻辑。首先检查成员是否已不在组内，或者是否已处于离开过程中。
     *           如果需要离开，则转换到 PREPARE_LEAVING 状态，并根据 `runCallbacks` 参数决定是否执行回调。
     *           回调完成后（或跳过回调后），会清除分配并转换到 LEAVING 状态，准备发送离开组的心跳。
     *           使用 `CompletableFuture` 来处理异步操作和结果通知。
     */
    protected CompletableFuture<Void> leaveGroup(boolean runCallbacks) {
        // 检查成员是否不在组内 (例如，状态为 UNSUBSCRIBED 或 FENCED)
        if (isNotInGroup()) {
            // 如果成员状态是 FENCED (被隔离)
            if (state == MemberState.FENCED) {
                // 清除当前分配
                clearAssignment();
                // 将状态转换为 UNSUBSCRIBED
                transitionTo(MemberState.UNSUBSCRIBED);
            }
            // 取消订阅所有主题
            subscriptions.unsubscribe();
            // 通知分配已更改为空集合
            notifyAssignmentChange(Collections.emptySet());
            // 返回一个已完成的 Future，因为成员已不在组内，无需执行离开操作
            return CompletableFuture.completedFuture(null);
        }

        // 如果成员已处于 PREPARE_LEAVING 或 LEAVING 状态
        if (state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING) {
            // 成员已经在离开过程中。不执行任何操作，并返回现有的离开组 Future，
            // 该 Future 将在正在进行的离开操作完成时完成。
            log.debug("Leave group operation already in progress for member {}", memberId);
            // 返回当前正在进行的离开操作的 Future
            return leaveGroupInProgress.get();
        }

        // 将成员状态转换为 PREPARE_LEAVING，准备释放分配
        transitionTo(MemberState.PREPARE_LEAVING);
        // 创建一个新的 CompletableFuture 用于表示离开组操作的结果
        CompletableFuture<Void> leaveResult = new CompletableFuture<>();
        // 将此 Future 保存到 leaveGroupInProgress，以便在重复调用时可以返回它
        leaveGroupInProgress = Optional.of(leaveResult);

        // 如果需要执行回调 (例如，在取消订阅时)
        if (runCallbacks) {
            // 发出信号，表示成员正在离开组，这将触发 onPartitionsRevoked 回调
            CompletableFuture<Void> callbackResult = signalMemberLeavingGroup();
            // 当回调完成时执行以下操作 (无论成功还是失败)
            callbackResult.whenComplete((result, error) -> {
                // 如果回调执行出错
                if (error != null) {
                    // 记录错误日志
                    log.error("Member {} callback to release assignment failed. It will proceed " +
                        "to clear its assignment and send a leave group heartbeat", memberId, error);
                } else {
                    // 如果回调执行成功，记录信息日志
                    log.info("Member {} completed callback to release assignment. It will proceed " +
                        "to clear its assignment and send a leave group heartbeat", memberId);
                }

                // 无论回调成功还是失败，都清除分配并准备离开组
                clearAssignmentAndLeaveGroup();
            });
        } else {
            // 如果不需要执行回调 (例如，在关闭消费者时)，直接清除分配并准备离开组
            clearAssignmentAndLeaveGroup();
        }

        // 返回 Future，表示当回调完成（如果执行）并且已转换为发送心跳的状态时，离开组操作完成。
        // 实际的离开组心跳将在后续的心跳循环中发送。
        return leaveResult;
    }

    /**
     * 清除分配并离开组。
     * 应用场景：当成员决定离开消费组时，调用此方法以清理资源并通知协调器。
     * 实现细节：首先取消订阅，然后清除本地分配，最后转换状态以发送离开组的心跳请求。
     * 设计考虑：确保成员在离开前完成必要的清理工作，并通过心跳通知协调器，以便协调器可以重新分配分区。
     */
    private void clearAssignmentAndLeaveGroup() {
        subscriptions.unsubscribe(); // 取消当前成员的所有订阅
        clearAssignment(); // 清除当前成员的本地分区分配信息

        // 转换状态以确保发送心跳请求从而有效地离开组
        // (即使在成员没有要释放的分配或回调执行失败的情况下也是如此)。
        transitionToSendingLeaveGroup(false); // 转换到发送离开组请求的状态，参数false表示不是因为轮询超时
    }

    /**
     * 将成员纪元（epoch）重置为离开组心跳请求所需的值，并转换到 {@link MemberState#LEAVING} 状态，
     * 以便发送带有该纪元的心跳请求。
     * 应用场景：当成员需要主动离开消费组时（例如取消订阅或关闭消费者），或者因为轮询超时而被动离开时，调用此方法。
     * 实现细节：
     * 1. 检查当前状态，如果处于 FATAL 或 UNSUBSCRIBED 状态，则不发送离开请求并记录警告。
     * 2. 如果是因为轮询超时（dueToExpiredPollTimer 为 true），则标记 isPollTimerExpired 为 true，并短暂转换到 PREPARE_LEAVING 状态。
     *    这种情况下，成员不需要在发送离开组请求前释放任何分配，因为它已经是 STALE 状态。它将在发送离开组请求后，在 STALE 状态下调用 onPartitionsLost。
     * 3. 更新成员纪元为离开组所需的纪元 (leaveGroupEpoch())。
     * 4. 将当前分配设置为空 (LocalAssignment.NONE)。
     * 5. 转换到 LEAVING 状态，准备发送离开组的心跳。
     * 设计考虑：
     * - 通过状态检查避免在不适当的状态下发送离开请求。
     * - 区分主动离开和因轮询超时导致的离开，后者有特殊的处理逻辑（保持 STALE 状态）。
     * - 更新成员纪元是离开组协议的关键部分，确保协调器能够正确处理离开请求。
     *
     * @param dueToExpiredPollTimer 如果离开组是由于轮询计时器过期，则为 True。这将表明成员在离开后必须保持 STALE 状态，
     *                              直到它释放其分配并且计时器被重置。
     */
    public void transitionToSendingLeaveGroup(boolean dueToExpiredPollTimer) {
        // 检查成员是否处于FATAL状态
        if (state == MemberState.FATAL) {
            // 如果是，记录警告日志，说明成员不会发送离开组请求，因为其处于FATAL状态
            log.warn("Member {} with epoch {} won't send leave group request because it is in " +
                    "FATAL state", memberId, memberEpoch);
            return; // 直接返回，不执行后续操作
        }
        // 检查成员是否处于UNSUBSCRIBED状态
        if (state == MemberState.UNSUBSCRIBED) {
            // 如果是，记录警告日志，说明成员不会发送离开组请求，因为它已经离开组
            log.warn("Member {} won't send leave group request because it is already out of the group.",
                memberId);
            return; // 直接返回，不执行后续操作
        }

        // 如果离开组是由于轮询计时器过期
        if (dueToExpiredPollTimer) {
            this.isPollTimerExpired = true; // 标记轮询计时器已过期
            // 短暂转换到 PREPARE_LEAVING 状态。成员不需要在发送离开组请求前释放任何分配，因为它已经是 STALE 状态。
            // 它将在发送离开组请求后，在 STALE 状态下调用 onPartitionsLost。
            transitionTo(MemberState.PREPARE_LEAVING); // 转换到准备离开状态
        }
        updateMemberEpoch(leaveGroupEpoch()); // 更新成员纪元为离开组所需的纪元
        currentAssignment = LocalAssignment.NONE; // 将当前分配设置为空
        transitionTo(MemberState.LEAVING); // 转换到离开组状态，准备发送心跳
    }

    /**
     * 调用所有已注册的监听器，以便在成员纪元（epoch）更新时得到通知。
     * 通知中也包含成员 ID。如果成员失败或离开组，将使用空纪元调用此方法。
     * 应用场景：当成员的 epoch 发生变化时（例如，成功加入组、被隔离、离开组），需要通知相关的监听器。
     * 实现细节：遍历 stateUpdatesListeners 列表，并为每个监听器调用 onMemberEpochUpdated 方法。
     * 设计考虑：提供一个统一的通知机制，以便其他组件可以响应成员 epoch 的变化。
     *
     * @param epoch 可选的成员纪元。如果成员成功加入或更新纪元，则包含新的纪元值；如果成员离开或失败，则为空。
     */
    void notifyEpochChange(Optional<Integer> epoch) {
        // 遍历所有状态更新监听器
        stateUpdatesListeners.forEach(stateListener -> 
            // 调用每个监听器的 onMemberEpochUpdated 方法，传入新的 epoch 和成员 ID
            stateListener.onMemberEpochUpdated(epoch, memberId)
        );
    }

    /**
     * 当分配的分区集合发生更改时，为每个监听器调用 {@link MemberStateListener#onGroupAssignmentUpdated(Set)} 回调。
     * 这包括分配更改、取消订阅以及离开组时。
     * 应用场景：当成员的实际分区分配发生变化时（例如，重平衡后分配了新的分区，或者取消订阅导致分区被撤销），需要通知相关的监听器。
     * 实现细节：遍历 stateUpdatesListeners 列表，并为每个监听器调用 onGroupAssignmentUpdated 方法。
     * 设计考虑：提供一个统一的通知机制，以便其他组件可以响应分区分配的变化。
     *
     * @param partitions 当前分配给成员的主题分区集合。
     */
    void notifyAssignmentChange(Set<TopicPartition> partitions) {
        // 遍历所有状态更新监听器
        stateUpdatesListeners.forEach(stateListener -> 
            // 调用每个监听器的 onGroupAssignmentUpdated 方法，传入新的分区集合
            stateListener.onGroupAssignmentUpdated(partitions)
        );
    }

    /**
     * 判断成员是否应该立即向协调器发送心跳，而无需等待心跳间隔。
     * 应用场景：在某些特定状态下，成员需要立即发送心跳以快速响应或完成某些操作，例如确认分配、离开组或加入组。
     * 实现细节：获取当前成员状态，如果状态是 ACKNOWLEDGING（确认分配中）、LEAVING（离开组中）或 JOINING（加入组中），则返回 true。
     * 设计考虑：优化心跳机制，在关键状态转换时允许立即发送心跳，以减少延迟。
     *
     * @return 如果成员应立即发送心跳，则为 True。
     */
    public boolean shouldHeartbeatNow() {
        MemberState state = state(); // 获取当前成员状态
        // 如果状态是 ACKNOWLEDGING、LEAVING 或 JOINING，则应立即发送心跳
        return state == MemberState.ACKNOWLEDGING || state == MemberState.LEAVING || state == MemberState.JOINING;
    }

    /**
     * 当生成心跳请求时更新状态。这将使成员从那些在发送心跳请求后即结束的状态（无需等待响应）转换出去，
     * 例如 {@link MemberState#ACKNOWLEDGING} 和 {@link MemberState#LEAVING}。
     * 应用场景：在发送了特定类型的心跳（如确认分配的心跳或离开组的心跳）后，成员的状态需要相应更新。
     * 实现细节：
     * - 如果当前状态是 ACKNOWLEDGING：
     *   - 如果目标分配已协调 (targetAssignmentReconciled() 返回 true)，则转换到 STABLE 状态。
     *   - 否则，记录调试日志并转换到 RECONCILING 状态，因为有新的分配需要协调。
     * - 如果当前状态是 LEAVING：
     *   - 如果是因为轮询超时 (isPollTimerExpired 为 true)，记录调试日志并转换到 STALE 状态。
     *   - 否则（主动离开），记录调试日志并转换到 UNSUBSCRIBED 状态。
     * 设计考虑：确保在发送了关键心跳后，成员状态能够及时、正确地更新，以反映其在组协议中的进展。
     */
    public void onHeartbeatRequestGenerated() {
        MemberState state = state(); // 获取当前成员状态
        // 如果当前状态是 ACKNOWLEDGING (正在确认分配)
        if (state == MemberState.ACKNOWLEDGING) {
            // 如果目标分配已经与当前分配一致 (已协调完成)
            if (targetAssignmentReconciled()) {
                transitionTo(MemberState.STABLE); // 转换到稳定状态
            } else {
                // 否则，目标分配尚未完全协调，记录调试信息并转换到 RECONCILING 状态
                log.debug("Member {} with epoch {} transitioned to {} after a heartbeat was sent " +
                        "to ack a previous reconciliation. New assignments are ready to " +
                        "be reconciled.", memberId, memberEpoch, MemberState.RECONCILING);
                transitionTo(MemberState.RECONCILING); // 转换到协调中状态
            }
        // 如果当前状态是 LEAVING (正在离开组)
        } else if (state == MemberState.LEAVING) {
            // 如果是因为轮询计时器过期而离开
            if (isPollTimerExpired) {
                // 记录调试信息，成员将保持 STALE 状态直到下次 poll 时重新加入组
                log.debug("Member {} with epoch {} generated the heartbeat to leave due to expired poll timer. It will " +
                    "remain stale (no heartbeat) until it rejoins the group on the next consumer " +
                    "poll.", memberId, memberEpoch);
                transitionToStale(); // 转换到 STALE 状态
            } else {
                // 否则是主动离开组
                log.debug("Member {} with epoch {} generated the heartbeat to leave the group.", memberId, memberEpoch);
                transitionTo(MemberState.UNSUBSCRIBED); // 转换到未订阅状态
            }
        }
    }

    /**
     * 即使心跳未发送，也从 {@link MemberState#LEAVING} 状态转换出去。
     * 这将确保成员不会阻塞在 {@link MemberState#LEAVING} 状态（尽力发送请求，无需任何响应处理或重试逻辑）。
     * 应用场景：当成员处于 LEAVING 状态，但由于某些原因（例如协调器不可用）无法发送离开组的心跳时，此方法确保成员最终能转换到 UNSUBSCRIBED 状态，避免无限期阻塞。
     * 实现细节：
     * - 检查当前状态是否为 LEAVING。
     * - 如果是，记录警告日志，说明无法发送离开组的心跳，并将成员状态转换为 UNSUBSCRIBED。
     * - 调用 maybeCompleteLeaveInProgress() 来完成可能正在进行的离开操作。
     * 设计考虑：这是一种容错机制，确保即使在网络或协调器出现问题时，成员的离开流程也能最终完成，避免状态卡死。
     */
    public void onHeartbeatRequestSkipped() {
        // 如果当前状态是 LEAVING (正在离开组)
        if (state == MemberState.LEAVING) {
            // 记录警告日志，说明离开组的心跳无法发送（很可能是因为协调器未知或不可用）
            log.warn("Heartbeat to leave group cannot be sent (most probably due to coordinator " +
                    "not known/available). Member {} with epoch {} will transition to {}.",
                memberId, memberEpoch, MemberState.UNSUBSCRIBED);
            transitionTo(MemberState.UNSUBSCRIBED); // 将成员状态转换为 UNSUBSCRIBED (未订阅)
            maybeCompleteLeaveInProgress(); // 尝试完成正在进行的离开组操作
        }
    }

    /**
     * 判断是否没有等待从元数据解析或等待协调的分配。
     * 应用场景：在确认分配（ACKNOWLEDGING 状态）后，需要检查目标分配是否已经完全应用到当前分配。
     * 实现细节：比较 currentAssignment（当前成员实际持有的分配）和 currentTargetAssignment（协调器下发的期望分配）是否相等。
     * 设计考虑：这是判断协调过程是否完成的关键逻辑。
     *
     * @return 如果没有等待解析或协调的分配，则为 True。
     */
    private boolean targetAssignmentReconciled() {
        // 比较当前分配 (currentAssignment) 与当前目标分配 (currentTargetAssignment) 是否相等
        return currentAssignment.equals(currentTargetAssignment);
    }

    /**
     * 判断成员是否不应发送心跳。
     * 当成员处于非活动成员状态时（例如，未订阅、致命错误、过时、被隔离），不应发送心跳。
     * 应用场景：控制心跳的发送逻辑，避免在不适当的状态下发送心跳，减少不必要的网络流量和服务器负载。
     * 实现细节：获取当前成员状态，如果状态是 UNSUBSCRIBED、FATAL、STALE 或 FENCED，则返回 true。
     * 设计考虑：明确定义了哪些状态下成员不需要发送心跳，简化了心跳调度逻辑。
     *
     * @return 如果成员不应发送心跳，则为 True。
     */
    public boolean shouldSkipHeartbeat() {
        MemberState state = state(); // 获取当前成员状态
        // 如果状态是 UNSUBSCRIBED (未订阅)、FATAL (致命错误)、STALE (过时) 或 FENCED (被隔离)，则不应发送心跳
        return state == MemberState.UNSUBSCRIBED ||
            state == MemberState.FATAL ||
            state == MemberState.STALE ||
            state == MemberState.FENCED;
    }

    /**
     * 判断成员是否正在准备离开组（等待回调执行）或正在离开组（发送最后的心跳）。
     * 此方法用于在消费者轮询计时器过期时，跳过主动离开组的逻辑（如果成员已经在离开过程中）。
     * 应用场景：避免在成员已经处于离开流程时，由于轮询超时再次触发离开组的逻辑，导致重复操作或状态冲突。
     * 实现细节：获取当前成员状态，如果状态是 PREPARE_LEAVING（准备离开）或 LEAVING（离开中），则返回 true。
     * 设计考虑：处理并发或重叠的离开组请求，确保离开流程的平稳执行。
     *
     * @return 如果成员正在准备离开组或正在离开组，则为 True。
     */
    public boolean isLeavingGroup() {
        MemberState state = state(); // 获取当前成员状态
        // 如果状态是 PREPARE_LEAVING (准备离开) 或 LEAVING (离开中)，则表示成员正在离开组
        return state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING;
    }

    /**
     * 当一个处于 {@link MemberState#STALE} 状态的成员完成其分配的释放时，将其转换为 {@link MemberState#JOINING} 状态。
     * 这预计在轮询计时器被重置时使用。
     * 应用场景：当消费者因长时间未轮询而被标记为STALE后，如果应用程序恢复轮询（重置轮询计时器），
     *          并且该成员已经完成了其先前分配的释放，则此方法会尝试让该成员重新加入消费组。
     * 实现细节：首先将 isPollTimerExpired 标志设置为 false，表明轮询计时器不再过期。
     *          然后检查当前成员状态是否为 STALE。如果是，则记录一条调试日志，
     *          并为 staleMemberAssignmentRelease（一个表示分配释放完成的 CompletableFuture）注册一个回调，
     *          该回调在分配释放完成后调用 transitionToJoining() 方法，使成员尝试重新加入组。
     * 设计考虑：确保只有在轮询计时器确实被重置且成员处于STALE状态时才尝试重新加入，
     *          并且重新加入的操作必须在先前的分配完全释放之后进行，以避免状态冲突或资源泄漏。
     */
    public void maybeRejoinStaleMember() {
        // 将轮询计时器过期标志设置为false，表示计时器已被重置
        isPollTimerExpired = false;
        // 检查当前成员状态是否为STALE
        if (state == MemberState.STALE) {
            // 记录调试信息，表明过期的轮询计时器已重置，STALE成员将在完成释放先前分配后重新加入组
            log.debug("过期的轮询计时器已重置，因此STALE成员 {} 将在完成释放其先前分配后重新加入组", memberId);
            // 当陈旧成员的分配释放操作完成时（无论成功或失败），调用transitionToJoining()方法尝试转换到JOINING状态
            staleMemberAssignmentRelease.whenComplete((__, error) -> transitionToJoining());
        }
    }

    /**
     * 转换为STALE状态以释放分配，因为成员由于轮询计时器过期而离开了组。
     * 这将触发onPartitionsLost回调。一旦回调完成，成员将保持STALE状态，
     * 直到轮询计时器被应用程序轮询事件重置。参见 {@link #maybeRejoinStaleMember()}。
     * 应用场景：当消费者的 poll() 方法长时间未被调用，导致其在消费组中的心跳超时，
     *          协调器会认为该成员已离开。此时，客户端需要将自身状态转换为 STALE，
     *          释放其当前持有的分区分配，并通知应用程序分区已丢失。
     * 实现细节：首先调用 transitionTo(MemberState.STALE) 将成员状态设置为 STALE。
     *          然后，调用 signalPartitionsLost() 方法触发 onPartitionsLost 回调，
     *          传递当前已分配的分区。此方法返回一个 CompletableFuture，表示回调的完成。
     *          将这个 CompletableFuture 赋值给 staleMemberAssignmentRelease，并注册一个回调：
     *          - 如果 onPartitionsLost 回调执行出错，记录错误日志。
     *          - 调用 clearAssignment() 清除本地的分配信息。
     *          - 记录调试日志，表明成员已发送离开组心跳并释放了分配，将保持 STALE 状态直到轮询计时器重置。
     * 设计考虑：确保在成员因轮询超时离开组时，能够正确地释放资源并通知应用程序。
     *          使用 CompletableFuture 来处理异步的回调完成，并在回调完成后清理分配状态。
     *          staleMemberAssignmentRelease 用于在后续 maybeRejoinStaleMember 中判断是否可以安全地重新加入。
     */
    private void transitionToStale() {
        // 将成员状态转换为STALE
        transitionTo(MemberState.STALE);

        // 释放分配
        // 调用signalPartitionsLost，通知监听器分区已丢失，并获取回调结果的CompletableFuture
        CompletableFuture<Void> callbackResult = signalPartitionsLost(subscriptions.assignedPartitions());
        // 将回调结果的CompletableFuture赋值给staleMemberAssignmentRelease，并注册完成时的操作
        staleMemberAssignmentRelease = callbackResult.whenComplete((result, error) -> {
            // 检查onPartitionsLost回调是否出错
            if (error != null) {
                // 如果出错，记录错误日志
                log.error("成员因轮询计时器过期离开组后，在释放分配时调用onPartitionsLost回调失败。", error);
            }
            // 清除当前分配信息
            clearAssignment();
            // 记录调试信息，表明成员已发送离开组心跳并释放了分配，将保持STALE状态直到轮询计时器重置，然后重新加入组
            log.debug("成员 {} 已发送离开组心跳并释放了其分配。它将保持在 {} 状态，直到轮询计时器被重置，然后它将重新加入组",
                memberId, MemberState.STALE);
        });
    }

    /**
     * 协调从服务器收到的分配。如果某些主题的topic ID无法匹配到主题名称，
     * 将触发元数据更新，并且只有可解析的主题子集会被协调。协调将触发回调并更新订阅状态。
     *
     * 在以下三种情况下不会触发协调：
     *  - 我们已经协调了分配（目标分配与当前分配相同）。
     *  - 另一个协调已在进行中。
     *  - 有些主题尚未添加到当前分配中，但它们的所有topic ID都从目标分配中缺失。
     * 应用场景：当消费者从协调器（broker）接收到新的分区分配时，此方法被调用以处理这个新分配。
     *          它负责确保本地状态与服务器期望的状态一致，包括更新订阅、调用用户回调等。
     * 实现细节：
     * 1. 前置检查：
     *    - 如果目标分配已经被协调（`targetAssignmentReconciled()`），则直接返回。
     *    - 如果已有协调正在进行中（`reconciliationInProgress`），则直接返回，当前分配将在下一个协调循环中处理。
     * 2. 解析分配：
     *    - 调用 `findResolvableAssignmentAndTriggerMetadataUpdate()` 查找目标分配中可以解析为主题名称的子集。
     *      如果存在无法解析的Topic ID，会触发元数据更新请求。
     *    - 基于可解析的分区创建一个 `LocalAssignment` 对象 `resolvedAssignment`。
     * 3. 特殊情况处理：
     *    - 如果当前分配非空，并且可解析的目标分配与当前分配的分区完全相同（但可能epoch不同，或者存在未解析分区），
     *      则只更新当前分配的epoch，并转换到 `ACKNOWLEDGING` 状态，然后返回。这用于处理部分解析但分区不变的情况。
     * 4. 开始协调：
     *    - 调用 `markReconciliationInProgress()` 将 `reconciliationInProgress` 标记为 true。
     *    - 计算需要添加的分区（`addedPartitions`）和需要撤销的分区（`revokedPartitions`）。
     *    - 记录详细的协调信息日志。
     *    - 调用 `markPendingRevocationToPauseFetching()` 标记待撤销分区，以暂停从这些分区的拉取。
     * 5. 提交偏移量并执行回调：
     *    - 调用 `signalReconciliationStarted()`，这通常会触发自动提交（如果启用）。此方法返回一个 `CompletableFuture`。
     *    - 在上述 `CompletableFuture` 完成后（`whenComplete`）：
     *        - 如果提交出错，记录错误日志，但协调仍会继续。
     *        - 如果提交成功，记录调试日志。
     *        - 调用 `maybeAbortReconciliation()` 检查协调是否应中止（例如，成员状态已改变）。
     *        - 如果未中止，则调用 `revokeAndAssign()` 执行实际的分区撤销和分配回调。
     *    - 使用 `exceptionally` 处理 `signalReconciliationStarted()` 可能抛出的异常，记录错误。
     * 设计考虑：
     * - 协调过程是异步的，涉及多个步骤（元数据解析、偏移量提交、用户回调）。
     * - 必须处理分配中包含无法立即解析的Topic ID的情况，通过触发元数据更新来解决。
     * - 协调过程应该是可中断的，以应对成员状态变化（如被踢出组）。
     * - 日志记录对于追踪复杂的协调流程至关重要。
     * - `reconciliationInProgress` 标志用于防止并发协调。
     */
    void maybeReconcile() {
        // 检查目标分配是否已经协调完毕
        if (targetAssignmentReconciled()) {
            // 如果是，则记录追踪日志并返回，忽略本次协调尝试
            log.trace("忽略协调尝试。目标分配与当前分配相同。");
            return;
        }
        // 检查是否已有另一个协调正在进行中
        if (reconciliationInProgress) {
            // 如果是，则记录追踪日志并返回，当前分配将在下一个协调循环中处理
            log.trace("忽略协调尝试。另一个协调已在进行中。分配 {} 将在下一个协调循环中处理。", currentTargetAssignment);
            return;
        }

        // 查找目标分配中可解析为主题名称的子集，如果某些topic ID无法解析，则触发元数据更新
        SortedSet<TopicIdPartition> assignedTopicIdPartitions = findResolvableAssignmentAndTriggerMetadataUpdate();
        // 基于可解析的分区和当前目标分配的epoch创建本地分配对象
        final LocalAssignment resolvedAssignment = new LocalAssignment(currentTargetAssignment.localEpoch, assignedTopicIdPartitions);

        // 如果当前分配不是空的，并且解析后的分配的分区集合与当前分配的分区集合相同
        // （这可能意味着存在未解析的分区，但已解析的部分与当前分配一致）
        if (!currentAssignment.isNone() && resolvedAssignment.partitions.equals(currentAssignment.partitions)) {
            // 记录调试信息，表明存在未解析的分区，但可解析的目标分配片段与当前分配相同
            // 将提升分配的本地epoch并确认部分解析的分配
            log.debug("存在未解析的分区，并且目标分配 {} 的可解析片段与当前分配相同。提升分配的本地epoch并确认部分解析的分配",
                resolvedAssignment.partitions);
            // 更新当前分配为解析后的分配（主要是更新epoch）
            currentAssignment = resolvedAssignment;
            // 转换到ACKNOWLEDGING状态，准备发送确认
            transitionTo(MemberState.ACKNOWLEDGING);
            return;
        }

        // 标记协调正在进行中
        markReconciliationInProgress();

        // 保留从TopicIdPartitions创建的已分配TopicPartitions的副本，这些分区正在被协调。
        // 这对于与尚不支持topic ID的集中式订阅状态以及回调的交互是必需的。
        SortedSet<TopicPartition> assignedTopicPartitions = toTopicPartitionSet(assignedTopicIdPartitions);
        // 创建一个用于存储当前拥有分区的有序集合，使用TOPIC_PARTITION_COMPARATOR进行排序
        SortedSet<TopicPartition> ownedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        // 将订阅状态中已分配的分区添加到ownedPartitions集合中
        ownedPartitions.addAll(subscriptions.assignedPartitions());

        // 计算需要分配的分区（新分配的分区中不包含在当前拥有的分区中的部分）
        SortedSet<TopicPartition> addedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        // 将所有新分配的分区添加到addedPartitions
        addedPartitions.addAll(assignedTopicPartitions);
        // 移除当前已拥有的分区，剩下的就是需要新增的
        addedPartitions.removeAll(ownedPartitions);

        // 计算需要撤销的分区（当前拥有的分区中不包含在新分配的分区中的部分）
        SortedSet<TopicPartition> revokedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        // 将所有当前拥有的分区添加到revokedPartitions
        revokedPartitions.addAll(ownedPartitions);
        // 移除新分配中依然存在的分区，剩下的就是需要撤销的
        revokedPartitions.removeAll(assignedTopicPartitions);

        // 记录协调分配的详细信息
        log.info("使用本地epoch {} 协调分配\n" +
                        "\t成员:                                    {}\n" +
                        "\t已分配分区:                       {}\n" +
                        "\t当前拥有分区:                  {}\n" +
                        "\t新增分区 (已分配 - 拥有):       {}\n" +
                        "\t撤销分区 (拥有 - 已分配):     {}\n",
                resolvedAssignment.localEpoch, // 本地epoch
                memberId, // 成员ID
                assignedTopicPartitions, // 新分配的分区 (TopicIdPartition格式)
                ownedPartitions, // 当前拥有的分区 (TopicPartition格式)
                addedPartitions, // 需要新增的分区
                revokedPartitions // 需要撤销的分区
        );

        // 标记待撤销分区以暂停从这些分区获取数据（不发送新的获取请求，也不处理进行中的获取响应）。
        markPendingRevocationToPauseFetching(revokedPartitions);

        // 如果启用了自动提交，在协调新分配之前提交偏移量。请求将重试，直到成功、发生不可重试错误或计时器到期。
        CompletableFuture<Void> commitResult;

        // 发出协调开始信号，这通常会触发偏移量提交（如果启用了自动提交）
        commitResult = signalReconciliationStarted();

        // 执行 提交 -> onPartitionsRevoked -> onPartitionsAssigned 的流程
        commitResult.whenComplete((__, commitReqError) -> {
            // 检查提交请求是否出错
            if (commitReqError != null) {
                // 提交调用（包括可重试错误的重试逻辑）未能在时间限制内完成（致命错误或未恢复的可重试错误）。继续执行撤销操作。
                log.error("协调新分配前的自动提交请求失败。无论如何都将继续协调。", commitReqError);
            } else {
                // 记录调试信息，表明协调新分配前的自动提交已成功完成
                log.debug("协调新分配前的自动提交已成功完成。");
            }

            // 检查是否应该中止协调（例如，成员状态已改变）
            if (!maybeAbortReconciliation()) {
                // 如果不中止，则执行分区的撤销和分配回调
                revokeAndAssign(resolvedAssignment, assignedTopicIdPartitions, revokedPartitions, addedPartitions);
            }

        }).exceptionally(error -> {
            // 捕获并处理signalReconciliationStarted()或其后续操作中可能发生的异常
            if (error != null) {
                // 记录协调失败的错误日志
                log.error("协调失败。", error);
            }
            // 返回null以表示异常已处理
            return null;
        });
    }

    /**
     * 根据给定的超时毫秒数计算截止时间点。
     * 应用场景：用于确定一个操作（例如，等待某个异步任务完成）的最晚完成时间。
     *          常用于需要设置超时的场景，如等待网络请求响应、等待回调完成等。
     * 实现细节：获取当前时间（`time.milliseconds()`），加上指定的超时时间 `timeoutMs`。
     *          如果计算得到的截止时间因为长整型溢出而变为负数，则返回 `Long.MAX_VALUE`，
     *          表示一个非常遥远的未来，实际上等同于无限等待或一个极大的超时。
     *          否则，返回计算得到的截止时间。
     * 设计考虑：处理了时间戳加法可能导致的溢出问题，确保返回一个有效的截止时间。
     *          使用 `Long.MAX_VALUE` 作为溢出时的回退值，是一种常见的处理方式。
     * @param timeoutMs 超时时长，单位为毫秒。
     * @return 截止时间的时间戳，单位为毫秒。如果计算发生溢出，则返回 {@link Long#MAX_VALUE}。
     */
    long getDeadlineMsForTimeout(final long timeoutMs) {
        // 计算过期时间点：当前时间 + 超时时长
        long expiration = time.milliseconds() + timeoutMs;
        // 检查计算得到的过期时间是否因为长整型溢出而小于0
        if (expiration < 0) {
            // 如果溢出，则返回长整型的最大值，表示一个非常遥远的未来（近似于无限超时）
            return Long.MAX_VALUE;
        }
        // 返回正常的过期时间点
        return expiration;
    }

    /**
     * 如果有任何分区被撤销，则触发onPartitionsRevoked回调。如果成功，
     * 则继续触发onPartitionsAssigned（即使没有添加新分区），
     * 然后通过更新分配并进行适当的状态转换来完成协调。
     * 注意，如果这两个回调中的任何一个失败，协调都应该失败。
     * 应用场景：在 `maybeReconcile` 方法中，当确定了需要撤销和分配的分区后，此方法被调用来实际执行这些操作，
     *          并处理相关的用户回调。
     * 实现细节：
     * 1. 处理分区撤销：
     *    - 如果 `revokedPartitions` 集合不为空，则调用 `revokePartitions(revokedPartitions)` 来执行撤销逻辑
     *      （这通常包括调用 `onPartitionsRevoked` 回调）。此方法返回一个 `CompletableFuture<Void>`。
     *    - 如果 `revokedPartitions` 为空，则创建一个已完成的 `CompletableFuture`。
     * 2. 顺序执行分配：
     *    - 使用 `thenCompose` 将撤销操作的结果（`revocationResult`）与分配操作连接起来，确保它们按顺序执行。
     *    - 在 `thenCompose` 的 lambda 中，首先检查协调是否应中止（`maybeAbortReconciliation()`）。
     *    - 如果不中止，则调用 `assignPartitions(assignedTopicIdPartitions, addedPartitions)` 来执行分配逻辑
     *      （这通常包括调用 `onPartitionsAssigned` 回调）。此方法也返回一个 `CompletableFuture<Void>`。
     *    - 如果需要中止，则返回一个已完成的 `CompletableFuture`。
     *    - `reconciliationResult` 是一个表示整个撤销和分配过程完成的 `CompletableFuture`。
     * 3. 处理最终结果：
     *    - 为 `reconciliationResult` 注册一个 `whenComplete` 回调来处理最终的成功或失败情况。
     *    - 如果发生错误（`error != null`）：
     *        - 记录协调失败的错误日志。
     *        - 调用 `markReconciliationCompleted()` 清理协调状态。
     *        - 注释中提到，回调失败后成员会保持在 `RECONCILING` 状态，不会发送确认，
     *          期望broker在协调提交超时后将成员踢出组，导致 `RECONCILING -> FENCED` 转换。
     *    - 如果没有错误：
     *        - 再次检查协调是否仍在进行中（`reconciliationInProgress`）并且不应中止（`!maybeAbortReconciliation()`）。
     *        - 如果条件满足，则更新 `currentAssignment` 为 `resolvedAssignment`。
     *        - 调用 `signalReconciliationCompleting()` 发出协调即将完成的信号。
     *        - 调用 `transitionTo(MemberState.ACKNOWLEDGING)` 将成员状态转换为准备发送确认。
     *        - 调用 `markReconciliationCompleted()` 清理协调状态。
     * 设计考虑：
     * - 使用 `CompletableFuture` 和 `thenCompose` 来优雅地处理异步操作的顺序执行和依赖关系。
     * - 确保在执行用户回调（撤销和分配）之前和之后都检查协调是否应该中止，以应对并发的状态变化。
     * - 详细处理了回调失败的情况，并解释了预期的后果（成员被隔离）。
     * - 协调完成后，正确更新成员状态并清理协调标志。
     *
     * @param resolvedAssignment 已解析的本地分配，包含新的epoch和可解析的分区。
     * @param assignedTopicIdPartitions 从服务器接收到的、已解析为TopicIdPartition的完整目标分配分区集合。
     * @param revokedPartitions 需要调用onPartitionsRevoked回调的已撤销分区集合 (TopicPartition格式)。
     * @param addedPartitions 需要调用onPartitionsAssigned回调的新增分区集合 (TopicPartition格式)。
     */
    private void revokeAndAssign(LocalAssignment resolvedAssignment,
                                 SortedSet<TopicIdPartition> assignedTopicIdPartitions,
                                 SortedSet<TopicPartition> revokedPartitions,
                                 SortedSet<TopicPartition> addedPartitions) {
        // 用于存储分区撤销操作结果的CompletableFuture
        CompletableFuture<Void> revocationResult;
        // 检查是否有需要撤销的分区
        if (!revokedPartitions.isEmpty()) {
            // 如果有，则调用revokePartitions执行撤销操作（包括回调）
            revocationResult = revokePartitions(revokedPartitions);
        } else {
            // 如果没有需要撤销的分区，则创建一个已完成的CompletableFuture
            revocationResult = CompletableFuture.completedFuture(null);
        }

        // 创建一个CompletableFuture，它将在整个协调过程（撤销和分配，按顺序执行）完成时完成。
        CompletableFuture<Void> reconciliationResult =
            // 使用thenCompose确保在撤销操作完成后再执行分配操作
            revocationResult.thenCompose(__ -> {
                // 检查协调是否应该中止
                if (!maybeAbortReconciliation()) {
                    // 如果不中止，则应用分配，调用assignPartitions执行分配操作（包括回调）
                    return assignPartitions(assignedTopicIdPartitions, addedPartitions);
                }
                // 如果需要中止，则返回一个已完成的CompletableFuture
                return CompletableFuture.completedFuture(null);
            });

        // 为整个协调结果注册完成时的回调
        reconciliationResult.whenComplete((__, error) -> {
            // 检查协调过程中是否发生错误
            if (error != null) {
                // 回调失败后，成员将保持在RECONCILING状态。成员不会发送ack，
                // 期望是broker在协调提交超时后将成员踢出组，导致RECONCILING -> FENCED的转换。
                log.error("协调失败。", error);
                // 标记协调已完成（即使是失败）
                markReconciliationCompleted();
            } else {
                // 如果没有错误，并且协调仍在进行中且不应中止
                if (reconciliationInProgress && !maybeAbortReconciliation()) {
                    // 更新当前分配为已解决的分配
                    currentAssignment = resolvedAssignment;

                    // 发出协调即将完成的信号
                    signalReconciliationCompleting();

                    // 通过转换到发送确认状态，使分配在broker上生效。
                    transitionTo(MemberState.ACKNOWLEDGING);
                    // 标记协调已完成
                    markReconciliationCompleted();
                }
            }
        });
    }

    /**
     * @return 如果正在进行的协调不应继续，则返回 True。这可能是因为
     * 成员不再处于 RECONCILING 状态（成员失败或正在离开组），或者
     * 如果它已重新加入组（请注意，重新加入后成员可能再次处于 RECONCILING 状态，
     * 因此仅检查状态是不够的）
     * 
     * 应用场景：在协调过程中，判断是否需要中止当前的协调操作。
     * 实现细节：检查成员状态是否为 RECONCILING，以及在协调过程中是否发生了重新加入组的情况。
     * 设计考虑：确保协调操作的有效性，避免在不适当的成员状态下继续执行协调，或应用过时的协调结果。
     */
    boolean maybeAbortReconciliation() { // 方法：可能中止协调
        // 检查成员状态是否不是 RECONCILING，或者在协调过程中是否已重新加入组
        boolean shouldAbort = state != MemberState.RECONCILING || rejoinedWhileReconciliationInProgress;
        // 如果应该中止
        if (shouldAbort) { // 条件：判断是否应该中止协调
            // 根据中止原因构建日志信息
            String reason = rejoinedWhileReconciliationInProgress ?
                "成员已重新加入组" :
                "成员已从协调状态转变为 " + state;
            // 记录中断协调的日志信息
            log.info("中断不再相关的协调，因为 " + reason);
            // 标记协调已完成（或被中止）
            markReconciliationCompleted();
        }
        // 返回是否应该中止协调
        return shouldAbort;
    } // 方法结束：maybeAbortReconciliation

    // 仅用于测试。
    /**
     * 更新当前分配。
     * 
     * 应用场景：在测试场景下，手动设置成员的当前分区分配。
     * 实现细节：创建一个新的 LocalAssignment 对象，并将其赋值给 currentAssignment 字段。
     * 设计考虑：提供一个便捷的方式来模拟不同的分配场景，以便进行单元测试。
     * @param partitions 要分配给此成员的分区，按主题 ID 映射到分区集合。
     */
    void updateAssignment(Map<Uuid, SortedSet<Integer>> partitions) { // 方法：更新分配
        // 使用提供的分区创建一个新的本地分配对象，epoch 设置为0
        currentAssignment = new LocalAssignment(0, partitions);
    } // 方法结束：updateAssignment

    /**
     * 通知成员资格管理器协调已开始，以便可以执行特定于组类型的操作。
     * 
     * 应用场景：在协调过程开始时，允许子类执行一些初始化或准备工作。
     * 实现细节：默认实现返回一个已完成的 CompletableFuture，子类可以重写此方法以执行异步操作。
     * 设计考虑：提供一个扩展点，允许不同类型的成员管理器在协调开始时执行自定义逻辑。
     * @return 一个 CompletableFuture，当特定于组类型的启动操作完成时，该 Future 将完成。
     */
    protected CompletableFuture<Void> signalReconciliationStarted() { // 方法：通知协调已开始
        // 默认返回一个已完成的 CompletableFuture，表示没有特定的启动操作
        return CompletableFuture.completedFuture(null);
    } // 方法结束：signalReconciliationStarted

    /**
     * 通知成员资格管理器协调即将完成，以便可以执行特定于组类型的操作。
     * 
     * 应用场景：在协调过程即将结束时，允许子类执行一些清理或收尾工作。
     * 实现细节：默认实现为空，子类可以重写此方法以执行特定操作。
     * 设计考虑：提供一个扩展点，允许不同类型的成员管理器在协调结束前执行自定义逻辑。
     */
    protected void signalReconciliationCompleting() { // 方法：通知协调即将完成
        // 默认实现为空，子类可以重写以添加特定逻辑
    } // 方法结束：signalReconciliationCompleting

    /**
     * 通知成员资格管理器成员正在离开组，以便可以执行特定于组类型的操作。
     * 
     * 应用场景：在成员离开组之前，允许子类执行一些特定于组类型的操作，例如资源释放或状态更新。
     * 实现细节：默认实现返回一个已完成的 CompletableFuture，子类可以重写此方法以执行异步操作。
     * 设计考虑：提供一个扩展点，允许不同类型的成员管理器在成员离开组时执行自定义逻辑。
     * @return 一个 CompletableFuture，当特定于组类型的离开操作完成时，该 Future 将完成。
     */
    protected CompletableFuture<Void> signalMemberLeavingGroup() { // 方法：通知成员正在离开组
        // 默认返回一个已完成的 CompletableFuture，表示没有特定的离开操作
        return CompletableFuture.completedFuture(null);
    } // 方法结束：signalMemberLeavingGroup

    /**
     * 通知成员资格管理器分配已丢失，以便可以执行特定于组类型的操作。
     * 
     * 应用场景：当成员的分区分配丢失时（例如，由于会话超时），允许子类执行特定于组类型的操作。
     * 实现细节：默认实现返回一个已完成的 CompletableFuture，子类可以重写此方法以执行异步操作。
     * 设计考虑：提供一个扩展点，允许不同类型的成员管理器在分区丢失时执行自定义逻辑。
     * @param partitionsLost 已丢失的分区集合。
     * @return 一个 CompletableFuture，当特定于组类型的分区丢失处理操作完成时，该 Future 将完成。
     */
    protected CompletableFuture<Void> signalPartitionsLost(Set<TopicPartition> partitionsLost) { // 方法：通知分区已丢失
        // 默认返回一个已完成的 CompletableFuture，表示没有特定的分区丢失处理操作
        return CompletableFuture.completedFuture(null);
    } // 方法结束：signalPartitionsLost

    /**
     * 从给定的 {@link TopicIdPartition} 集合构建 {@link TopicPartition} 集合。
     * 
     * 应用场景：将包含主题 ID 的分区表示转换为不包含主题 ID 的分区表示。
     * 实现细节：遍历输入的 TopicIdPartition 集合，对每个元素调用 topicPartition() 方法获取 TopicPartition 对象，并添加到结果集合中。
     * 设计考虑：提供一个工具方法，方便在不同分区表示之间进行转换。
     * @param topicIdPartitions 包含主题 ID、主题名称和分区 ID 的 TopicIdPartition 对象的有序集合。
     * @return 包含主题名称和分区 ID 的 TopicPartition 对象的有序集合。
     */
    protected SortedSet<TopicPartition> toTopicPartitionSet(SortedSet<TopicIdPartition> topicIdPartitions) { // 方法：将 TopicIdPartition 集合转换为 TopicPartition 集合
        // 创建一个使用 TOPIC_PARTITION_COMPARATOR 排序的 TreeSet 用于存储结果
        SortedSet<TopicPartition> result = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        // 遍历输入的 topicIdPartitions 集合
        topicIdPartitions.forEach(topicIdPartition -> 
            // 将每个 TopicIdPartition 对象转换为 TopicPartition 对象并添加到结果集中
            result.add(topicIdPartition.topicPartition()));
        // 返回转换后的 TopicPartition 集合
        return result;
    } // 方法结束：toTopicPartitionSet

    /**
     *  标记协调正在进行中。仅用于测试。
     * 
     * 应用场景：在测试中模拟协调过程的开始。
     * 实现细节：将 reconciliationInProgress 设置为 true，并将 rejoinedWhileReconciliationInProgress 设置为 false。
     * 设计考虑：提供一个受控的方式来设置协调状态，方便测试相关逻辑。
     */
    void markReconciliationInProgress() { // 方法：标记协调正在进行中
        // 将协调进行中标志设置为 true
        reconciliationInProgress = true;
        // 将协调过程中重新加入标志设置为 false
        rejoinedWhileReconciliationInProgress = false;
    } // 方法结束：markReconciliationInProgress

    /**
     *  标记协调已完成。仅用于测试。
     * 
     * 应用场景：在测试中模拟协调过程的结束。
     * 实现细节：将 reconciliationInProgress 和 rejoinedWhileReconciliationInProgress 都设置为 false。
     * 设计考虑：提供一个受控的方式来重置协调状态，方便测试相关逻辑。
     */
    void markReconciliationCompleted() { // 方法：标记协调已完成
        // 将协调进行中标志设置为 false
        reconciliationInProgress = false;
        // 将协调过程中重新加入标志设置为 false
        rejoinedWhileReconciliationInProgress = false;
    } // 方法结束：markReconciliationCompleted

    /**
     * 从代理接收的目标分配（主题 ID 和分区列表）构建 TopicIdPartition（主题 ID、主题名称和分区 ID）集合。
     *
     * <p>
     * 这将执行以下操作：
     *
     * <ol type="1">
     *     <li>尝试在元数据缓存中查找主题名称</li>
     *     <li>对于元数据中未找到的主题，尝试在本地主题名称缓存中查找名称
     *     （包含当前已分配和已解析的主题 ID 和名称）</li>
     *     <li>如果存在元数据缓存或分配给此成员的本地主题名称缓存中都未找到的主题，
     *     则请求元数据更新，并在缓存更新时继续解析名称。
     *     </li>
     * </ol>
     * 
     * 应用场景：当收到新的目标分配时，需要将仅包含主题 ID 的分配信息转换为包含主题名称的完整分配信息，以便后续处理。
     * 实现细节：首先尝试从全局元数据缓存或本地已分配主题名称缓存中解析主题名称。如果仍有未解析的主题，则触发元数据更新请求。
     * 设计考虑：尽可能利用现有缓存减少元数据请求，同时确保在必要时能获取到最新的主题名称信息。
     * @return 一个有序集合，包含已成功解析主题名称并准备好进行协调的 TopicIdPartition 对象。
     */
    private SortedSet<TopicIdPartition> findResolvableAssignmentAndTriggerMetadataUpdate() { // 方法：查找可解析的分配并触发元数据更新
        // 创建一个用于存储准备好协调的分配的 TreeSet，使用 TOPIC_ID_PARTITION_COMPARATOR 进行排序
        final SortedSet<TopicIdPartition> assignmentReadyToReconcile = new TreeSet<>(TOPIC_ID_PARTITION_COMPARATOR);
        // 创建一个 HashMap 用于存储未解析的主题分配，初始值为当前目标分配中的所有分区
        final HashMap<Uuid, SortedSet<Integer>> unresolved = new HashMap<>(currentTargetAssignment.partitions);

        // 尝试从元数据缓存或订阅缓存中解析主题名称，并将已解析的分配
        // 从 "unresolved" 集合移动到 "assignmentReadyToReconcile" 集合。
        // 获取 unresolved 集合的迭代器
        Iterator<Map.Entry<Uuid, SortedSet<Integer>>> it = unresolved.entrySet().iterator();
        // 遍历未解析的分配
        while (it.hasNext()) { // 循环：遍历未解析的分配
            // 获取当前未解析的分配条目
            Map.Entry<Uuid, SortedSet<Integer>> e = it.next();
            // 获取主题 ID
            Uuid topicId = e.getKey();
            // 获取该主题下的分区集合
            SortedSet<Integer> topicPartitions = e.getValue();

            // 尝试在全局或本地缓存中查找主题名称
            Optional<String> nameFromMetadata = findTopicNameInGlobalOrLocalCache(topicId);
            // 如果找到了主题名称
            nameFromMetadata.ifPresent(resolvedTopicName -> { // 条件：如果主题名称已解析
                // 主题名称已解析，因此该分配已准备好进行协调。
                // 遍历该主题下的所有分区
                topicPartitions.forEach(tp ->
                    // 创建 TopicIdPartition 对象并添加到 assignmentReadyToReconcile 集合中
                    assignmentReadyToReconcile.add(new TopicIdPartition(topicId, tp, resolvedTopicName))
                );
                // 从 unresolved 集合中移除已解析的分配
                it.remove();
            }); // 结束条件：主题名称已解析
        } // 结束循环：遍历未解析的分配

        // 如果仍有未解析的分配
        if (!unresolved.isEmpty()) { // 条件：如果存在未解析的分配
            // 记录调试日志，说明哪些主题 ID 在元数据中未找到且当前未分配，并请求元数据更新
            log.debug("目标分配中收到的主题 ID {} 在元数据中未找到，并且当前未分配。现在请求元数据更新以解析主题名称。", unresolved.keySet());
            // 请求元数据更新，参数 true 表示允许陈旧的元数据
            metadata.requestUpdate(true);
        } // 结束条件：存在未解析的分配

        // 返回准备好协调的分配集合
        return assignmentReadyToReconcile;
    } // 方法结束：findResolvableAssignmentAndTriggerMetadataUpdate

    /**
     * 在全局元数据缓存中查找主题。如果找到，则将其添加到本地缓存并返回。
     * 如果未找到，则在本地元数据缓存中查找。如果两者都未找到，则返回空。
     * 
     * 应用场景：根据主题 ID 解析主题名称，优先使用全局元数据缓存，其次是本地已分配主题名称缓存。
     * 实现细节：首先查询全局元数据缓存，如果找到则更新本地缓存并返回。否则，查询本地缓存。
     * 设计考虑：通过两级缓存机制，提高主题名称解析的效率和命中率，减少对外部元数据源的依赖。
     * @param topicId 要查找的主题的 ID。
     * @return 包含主题名称的 Optional 对象；如果未找到，则为空 Optional。
     */
    private Optional<String> findTopicNameInGlobalOrLocalCache(Uuid topicId) { // 方法：在全局或本地缓存中查找主题名称
        // 从元数据缓存中获取主题名称，如果不存在则返回 null
        String nameFromMetadataCache = metadata.topicNames().getOrDefault(topicId, null);
        // 如果从元数据缓存中找到了主题名称
        if (nameFromMetadataCache != null) { // 条件：如果从元数据缓存中找到主题名称
            // 将主题名称添加到本地缓存，以便在元数据缓存不可用时，如果下一个目标分配中包含该主题，则可以重用。
            assignedTopicNamesCache.put(topicId, nameFromMetadataCache);
            // 返回包含主题名称的 Optional 对象
            return Optional.of(nameFromMetadataCache);
        } else { // 否则（即元数据缓存中未找到）
            // 主题 ID 在元数据中未找到。检查主题名称是否存在于当前已分配主题的本地缓存中。
            // 这将避免在元数据缓存可能在撤销先前分配的主题之前被刷新的情况下发起元数据请求。
            // 从本地已分配主题名称缓存中获取主题名称
            String nameFromSubscriptionCache = assignedTopicNamesCache.getOrDefault(topicId, null);
            // 返回可能为 null 的主题名称的 Optional 对象
            return Optional.ofNullable(nameFromSubscriptionCache);
        } // 结束条件：从元数据缓存中找到主题名称
    } // 方法结束：findTopicNameInGlobalOrLocalCache

    /**
     * 撤销分区。这将：
     * <ul>
     *     <li>如果启用了自动提交，则触发异步提交偏移量请求。</li>
     *     <li>如果用户已注册，则调用 onPartitionsRevoked 回调。</li>
     * </ul>
     *
     * 这将在调用回调之前等待提交请求完成。如果提交请求失败，
     * 它仍将继续调用用户回调，并返回一个 future，该 future 将仅根据回调执行情况完成或失败。
     *
     * 应用场景：在分区重新分配或消费者离开组时，需要撤销当前分配给该消费者的分区。
     * 实现细节：首先确保要撤销的分区确实是当前已分配的。然后标记这些分区为待撤销状态以暂停拉取。接着，如果成员状态正常，则触发用户定义的分区撤销回调。
     * 设计考虑：撤销过程需要处理自动提交（如果启用）和用户回调。即使提交失败，也应尝试执行用户回调。通过 CompletableFuture 管理异步操作的完成状态。
     * @param partitionsToRevoke 要撤销的分区。
     * @return 当提交请求和用户回调完成时将完成的 Future。
     * 仅用于测试
     */
    CompletableFuture<Void> revokePartitions(Set<TopicPartition> partitionsToRevoke) { // 方法：撤销分区
        // 确保要撤销的分区集合仍然是已分配的
        // 创建一个要撤销分区的副本
        Set<TopicPartition> revokedPartitions = new HashSet<>(partitionsToRevoke);
        // 保留副本中实际已分配给当前订阅的分区
        revokedPartitions.retainAll(subscriptions.assignedPartitions());
        // 记录将要撤销的先前已分配的分区
        log.info("正在撤销先前分配的分区 {}", revokedPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));

        // 通知分区正在被撤销（这是一个可被子类重写的方法，用于执行特定于组类型的操作）
        signalPartitionsBeingRevoked(revokedPartitions);

        // 将分区标记为待撤销状态，以停止从这些分区获取数据（不会发送新的获取请求，也不会处理正在进行的获取响应）。
        markPendingRevocationToPauseFetching(revokedPartitions);

        // 用于表示撤销操作完成的 Future（包括偏移量提交请求和用户回调执行）。
        CompletableFuture<Void> revocationResult = new CompletableFuture<>();

        // 此时，我们期望正处于从 RECONCILING 或 PREPARE_LEAVING 状态触发的撤销过程中，
        // 但也可能成员在等待提交完成时收到了致命错误。检查是否是这种情况并中止撤销。
        // 如果成员状态为 FATAL
        if (state == MemberState.FATAL) { // 条件：如果成员状态为 FATAL
            // 构建错误消息
            String errorMsg = String.format("成员 %s (epoch %s) 在等待撤销提交完成时收到致命错误。将中止撤销而不触发用户回调。", memberId, memberEpoch);
            // 记录调试日志
            log.debug(errorMsg);
            // 以异常方式完成 revocationResult
            revocationResult.completeExceptionally(new KafkaException(errorMsg));
            // 返回 revocationResult
            return revocationResult;
        } // 结束条件：成员状态为 FATAL

        // 调用 signalPartitionsRevoked 方法，该方法会处理偏移量提交（如果需要）并调用用户提供的 onPartitionsRevoked 回调
        CompletableFuture<Void> userCallbackResult = signalPartitionsRevoked(revokedPartitions);
        // 当用户回调（以及可能的偏移量提交）完成后执行
        userCallbackResult.whenComplete((callbackResult, callbackError) -> { // 异步回调：用户回调完成时
            // 如果回调执行过程中发生错误
            if (callbackError != null) { // 条件：如果回调出错
                // 记录错误日志
                log.error("onPartitionsRevoked 回调调用失败，分区: {}",
                    revokedPartitions, callbackError);
                // 以异常方式完成 revocationResult
                revocationResult.completeExceptionally(callbackError);
            } else { // 否则（回调成功）
                // 正常完成 revocationResult
                revocationResult.complete(null);
            } // 结束条件：回调出错

        }); // 结束异步回调：用户回调完成时
        // 返回表示整个撤销操作（包括回调）的 Future
        return revocationResult;
    } // 方法结束：revokePartitions


    /**
     * 使新的分配生效，并为添加的分区触发 onPartitionsAssigned 回调。
     * 这还将更新本地主题名称缓存，从中删除所有不再分配给该成员的主题。
     * 这也确保在回调完成之前，不会为新添加的分区获取记录和初始化位置。
     *
     * 应用场景: 当消费者组成员收到新的分区分配时，此方法负责更新内部状态并通知应用程序。
     * 实现细节:
     * 1. 更新订阅状态以反映新的分配，并暂时阻止新分区的获取和位置初始化。
     * 2. 调用用户提供的 onPartitionsAssigned 回调。
     * 3. 根据回调结果，启用新分配分区的获取，或在失败时保持其不可获取状态。
     * 4. 清理主题名称缓存，移除不再分配的主题。
     * 设计考虑:
     * - 回调的异步执行: 使用 CompletableFuture 处理回调的异步完成。
     * - 错误处理: 如果回调失败，新添加的分区将保持不可获取状态，并在下一个协调循环中重试。
     * - 缓存管理: 及时清理不再需要的主题名称缓存，以避免内存泄漏和过时数据。
     * - 使用 assignedPartitions 而不是 addedPartitions 来启用分区获取: 这是为了处理回调可能抛出异常导致 addedPartitions 为空的情况，确保即使回调失败，如果后续协调成功，仍然可以拉取数据，与经典消费者的行为保持一致。
     *
     * @param assignedPartitions 将在成员订阅状态中更新的完整分配。
     *                           这包括先前拥有的和新添加的分区。
     * @param addedPartitions    新分配中包含的、该成员之前未拥有的分区。
     *                           这些分区将提供给 onPartitionsAssigned 回调。
     * @return 当回调执行完成时将完成的 Future。
     */
    private CompletableFuture<Void> assignPartitions(
            SortedSet<TopicIdPartition> assignedPartitions, // 参数：已分配的主题ID分区集合，表示成员当前应该拥有的所有分区
            SortedSet<TopicPartition> addedPartitions) { // 参数：新添加的主题分区集合，这些是本次分配中新增的分区

        // 更新订阅状态中的分配，并确保在回调运行时，不会对新添加的分区进行获取或位置初始化。
        // 实现细节: 调用 updateSubscriptionAwaitingCallback 方法，将新分配的分区标记为等待回调状态，
        // 这会阻止 Fetcher 获取这些分区的数据，直到回调成功完成。
        updateSubscriptionAwaitingCallback(assignedPartitions, addedPartitions);

        // 调用用户回调。
        // 实现细节: 调用 signalPartitionsAssigned 方法，该方法通常会触发用户定义的 ConsumerRebalanceListener#onPartitionsAssigned。
        CompletableFuture<Void> result = signalPartitionsAssigned(addedPartitions);
        // 使新添加的分区能够开始为它们获取和更新位置。
        // 实现细节: 使用 whenComplete 注册一个回调，在 signalPartitionsAssigned 返回的 Future 完成后执行。
        result.whenComplete((__, exception) -> { // 回调函数，处理 onPartitionsAssigned 的结果
            if (exception == null) { // 如果 onPartitionsAssigned 回调成功执行（没有异常）
                // 使已分配的分区能够开始为它们获取和更新位置。
                // 我们在这里使用 assignedPartitions 而不是 addedPartitions，因为回调有可能会抛出异常，导致 addedPartitions 为空。
                // 这将导致轮询操作不返回任何记录，因为没有主题分区被标记为可获取。
                // 相反，对于经典消费者，如果第一个回调失败但下一个回调成功，轮询仍然可以检索数据。
                // 为了与此行为保持一致，我们依赖 assignedPartitions 来避免此类情况。
                // 实现细节: 调用 subscriptions.enablePartitionsAwaitingCallback，允许 Fetcher 开始获取这些分区的数据。
                // 使用 toTopicPartitionSet(assignedPartitions) 将 TopicIdPartition 转换为 TopicPartition。
                subscriptions.enablePartitionsAwaitingCallback(toTopicPartitionSet(assignedPartitions));
            } else { // 如果 onPartitionsAssigned 回调执行失败
                // 在回调失败后，保持新添加的分区为不可获取状态。
                // 它们将在下一个协调循环中重试，直到成功或代理将它们从分配中移除。
                // 实现细节: 记录警告日志，说明这些分区将保持不可获取状态。
                if (!addedPartitions.isEmpty()) { // 检查是否有新添加的分区
                    log.warn("保持新分配的分区 {} 为不可获取状态，并且在 onPartitionsAssigned 回调失败后不初始化其位置。",
                        addedPartitions, exception); // 记录警告信息
                }
            }
        });

        // 清理主题名称缓存，删除不再分配给该成员的主题。
        // 实现细节: 获取所有已分配主题的名称集合。
        Set<String> assignedTopics = assignedPartitions.stream().map(TopicIdPartition::topic).collect(Collectors.toSet());
        // 实现细节: 从 assignedTopicNamesCache 中移除所有不在 assignedTopics 集合中的主题名称。
        assignedTopicNamesCache.values().retainAll(assignedTopics);

        return result; // 返回表示 onPartitionsAssigned 回调完成的 Future
    }

    /**
     * 通知成员资格管理器正在分配分区，以便可以采取特定于组类型的操作。
     *
     * 应用场景: 这是一个钩子方法，允许子类（如 ConsumerMembershipManager）在分区分配时执行特定逻辑，
     * 例如调用用户提供的 {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)}。
     * 实现细节: 默认实现返回一个已完成的 CompletableFuture，表示没有特定的操作。子类应重写此方法以实现其特定行为。
     * 设计考虑: 提供一个可扩展点，以适应不同类型的组成员资格管理器的需求。
     *
     * @param partitionsAssigned 已分配给此成员的分区集合。
     * @return 一个 CompletableFuture，当与分区分配相关的操作完成时，该 Future 将完成。
     */
    public CompletableFuture<Void> signalPartitionsAssigned(Set<TopicPartition> partitionsAssigned) { // 参数：已分配的主题分区集合
        // 默认实现，直接返回一个已完成的 CompletableFuture。
        // 子类（如 ConsumerMembershipManager）会重写此方法来实际调用用户的 onPartitionsAssigned 回调。
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 通知成员资格管理器正在撤销分区，以便可以采取特定于组类型的操作。
     *
     * 应用场景: 这是一个钩子方法，允许子类在分区即将被撤销时执行特定逻辑，
     * 例如，在调用用户提供的 {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)} 之前，
     * 可能需要执行一些清理操作或提交偏移量。
     * 实现细节: 默认实现为空方法。子类应重写此方法以实现其特定行为。
     * 设计考虑: 提供一个可扩展点，用于在分区撤销过程的早期阶段介入。
     *
     * @param partitionsToRevoke 即将被撤销的分区集合。
     */
    public void signalPartitionsBeingRevoked(Set<TopicPartition> partitionsToRevoke) { // 参数：将要被撤销的主题分区集合
        // 默认实现为空。
        // 子类（如 ConsumerMembershipManager）可能会重写此方法，
        // 例如，在调用 onPartitionsRevoked 之前执行一些预处理操作。
    }

    /**
     * 通知成员资格管理器分区已被撤销，以便可以采取特定于组类型的操作。
     *
     * 应用场景: 这是一个钩子方法，允许子类（如 ConsumerMembershipManager）在分区撤销完成后执行特定逻辑，
     * 例如调用用户提供的 {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)} 或
     * {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsLost(java.util.Collection)}。
     * 实现细节: 默认实现返回一个已完成的 CompletableFuture。子类应重写此方法以实现其特定行为。
     * 设计考虑: 提供一个可扩展点，用于在分区撤销过程的后期阶段介入。
     *
     * @param partitionsRevoked 已被撤销的分区集合。
     * @return 一个 CompletableFuture，当与分区撤销相关的操作完成时，该 Future 将完成。
     */
    public CompletableFuture<Void> signalPartitionsRevoked(Set<TopicPartition> partitionsRevoked) { // 参数：已被撤销的主题分区集合
        // 默认实现，直接返回一个已完成的 CompletableFuture。
        // 子类（如 ConsumerMembershipManager）会重写此方法来实际调用用户的 onPartitionsRevoked 或 onPartitionsLost 回调。
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 将分区标记为“待撤销”，以在等待提交偏移量请求完成期间有效停止获取，
     * 并确保应用程序的位置不会超过已提交的位置。此标记将确保：
     * <ul>
     *     <li>不会为正在撤销的分区发送新的获取请求</li>
     *     <li>在分区被撤销期间可能完成的先前进行中的获取请求将不会被处理。</li>
     * </ul>
     *
     * 应用场景: 在异步提交偏移量之后、分区撤销回调（onPartitionsRevoked）完成之前，存在一个时间窗口。
     * 在此期间，如果处理了这些待撤销分区的已拉取数据，可能导致消费位点超前于已提交位点，从而在重新分配后造成重复消费。
     * 此方法通过将分区标记为“待撤销”来阻止这种情况，Fetcher 会暂停对这些分区的拉取和数据返回。
     * 实现细节: 调用 {@link SubscriptionState#markPendingRevocation(Set)} 方法来更新分区的状态。
     * 设计考虑: 这是确保“至少一次”消费语义的关键步骤，特别是在异步提交和分区重平衡的交互过程中，防止数据丢失或重复处理。
     *
     * @param partitionsToRevoke 需要标记为待撤销的分区集合。
     */
    private void markPendingRevocationToPauseFetching(Set<TopicPartition> partitionsToRevoke) { // 参数：将要被撤销并暂停获取的主题分区集合
        // 当在撤销一组分区之前异步提交偏移量时，从发送偏移量提交到其返回并完成撤销之间会有一段时间窗口。
        // 在此期间，这些分区的待处理获取请求可能会返回，这意味着应用程序的位置可能会在撤销之前超过已提交的位置。这可能导致重复消费。
        // 为防止这种情况，我们将分区标记为“待撤销”，这将阻止 Fetcher 发送新的获取请求或向用户返回先前获取的数据。
        // 实现细节: 记录调试日志，标明哪些分区被标记为待撤销。
        log.debug("标记待撤销的分区: {}", partitionsToRevoke);
        // 实现细节: 调用 SubscriptionState 的 markPendingRevocation 方法，
        // 该方法会更新内部状态，使得 Fetcher 暂停对这些分区的获取操作。
        subscriptions.markPendingRevocation(partitionsToRevoke);
    }

    /**
     * 丢弃已收到但尚未协调的分配（等待元数据或下一个协调循环）。
     * 从主题名称缓存中删除所有元素。
     *
     * 应用场景: 当成员状态发生重大变化（例如，离开组、订阅改变、发生严重错误）时，
     * 需要清除所有待处理的分配信息和本地缓存，以确保状态的一致性。
     * 实现细节:
     * 1. 将当前目标分配（currentTargetAssignment）重置为 {@link LocalAssignment#NONE}。
     * 2. 清空已分配主题名称缓存（assignedTopicNamesCache）。
     * 设计考虑: 确保在状态转换时，不会残留过时的分配信息或缓存数据，避免潜在的逻辑错误。
     */
    private void clearPendingAssignmentsAndLocalNamesCache() {
        // 实现细节: 将当前目标分配设置为 LocalAssignment.NONE，表示没有待处理的目标分配。
        currentTargetAssignment = LocalAssignment.NONE;
        // 实现细节: 清空 assignedTopicNamesCache，移除所有缓存的主题ID到名称的映射。
        assignedTopicNamesCache.clear();
    }

    /**
     * 重置成员的 epoch。
     *
     * 应用场景: 当成员需要重新加入组时（例如，在被隔离后或初次加入时），其 epoch 需要被重置。
     * 实现细节: 调用 {@link #updateMemberEpoch(int)} 方法，并将新的 epoch 值设置为 {@link #joinGroupEpoch()} 的返回值。
     * {@link #joinGroupEpoch()} 是一个抽象方法，由子类实现，定义了加入组时应使用的 epoch 值（通常是0或-1，取决于具体的组协议）。
     * 设计考虑: 提供一个统一的方法来重置 epoch，具体的 epoch 值由子类根据其协议类型决定。
     */
    protected void resetEpoch() {
        // 实现细节: 调用 updateMemberEpoch 方法，使用 joinGroupEpoch() 返回的值作为新的 epoch。
        // joinGroupEpoch() 是一个抽象方法，由具体的成员资格管理器实现，用于获取加入组时使用的 epoch 值。
        updateMemberEpoch(joinGroupEpoch());
    }

    /**
     * 返回成员用于加入组的 epoch。这是特定于组类型的。
     *
     * 应用场景: 不同的消费者组协议可能对加入组时使用的 epoch 有不同的规定。
     * 例如，在经典组协议中，加入时 epoch 通常为 -1 (UNKNOWN_MEMBER_ID) 或 0。
     * 在新的消费者组协议 (KIP-848) 中，加入时 epoch 为 0。
     * 实现细节: 这是一个抽象方法，必须由具体的子类（如 {@link ConsumerMembershipManager} 或旧的 {@code AbstractCoordinator} 的子类）实现。
     * 设计考虑: 将特定于协议的逻辑委托给子类，保持基类的通用性。
     *
     * @return 用于加入组的 epoch。
     */
    abstract int joinGroupEpoch(); // 抽象方法：获取成员加入组时使用的 epoch

    /**
     * 返回成员用于离开组的 epoch。这是特定于组类型的。
     *
     * 应用场景: 类似于 {@link #joinGroupEpoch()}，离开组时使用的 epoch 也可能因协议而异。
     * 例如，在新的消费者组协议 (KIP-848) 中，干净地离开组时 epoch 为 -2 (LEAVE_GROUP_MEMBER_EPOCH)。
     * 实现细节: 这是一个抽象方法，必须由具体的子类实现。
     * 设计考虑: 将特定于协议的逻辑委托给子类。
     *
     * @return 用于离开组的 epoch。
     */
    abstract int leaveGroupEpoch(); // 抽象方法：获取成员离开组时使用的 epoch

    /**
     * 更新成员的 epoch。
     *
     * 应用场景: 当成员的 epoch 发生变化时（例如，成功加入组、被隔离、协调分配后），调用此方法更新内部状态并通知监听器。
     * 实现细节:
     * 1. 检查新的 epoch 是否与当前 epoch 不同。
     * 2. 更新 {@link #memberEpoch} 字段。
     * 3. 如果 epoch 确实发生了变化，则调用 {@code notifyEpochChange(Optional)} 通知相关的监听器或组件。
     *    - 如果新的 epoch 大于 0，则传递 {@code Optional.of(memberEpoch)}。
     *    - 否则（例如，epoch 为 0 或负数，表示未加入或被隔离），则传递 {@code Optional.empty()}。
     * 设计考虑:
     * - 仅在 epoch 实际改变时才触发通知，避免不必要的处理。
     * - 成员 ID 在启动时生成且在其整个生命周期内保持不变，因此通知仅基于 epoch 的变化。
     *
     * @param newEpoch 新的成员 epoch。
     */
    protected void updateMemberEpoch(int newEpoch) { // 参数：新的成员 epoch
        // 实现细节: 检查新 epoch 是否与当前 epoch 不同，以确定 epoch 是否真的发生了变化。
        boolean newEpochReceived = this.memberEpoch != newEpoch;
        // 实现细节: 更新成员的 epoch。
        this.memberEpoch = newEpoch;
        // 仅基于 epoch 的变化进行通知，因为成员将在启动时生成一个成员 ID，
        // 并且在其整个生命周期内保持不变。
        if (newEpochReceived) { // 如果 epoch 确实发生了变化
            if (memberEpoch > 0) { // 如果新的 epoch 是一个有效的正值（表示已成功加入并分配）
                // 实现细节: 通知 epoch 变化，并提供新的 epoch 值。
                notifyEpochChange(Optional.of(memberEpoch));
            } else { // 如果新的 epoch 不是正值（例如，0 或负数，表示未加入、被隔离或离开）
                // 实现细节: 通知 epoch 变化，但不提供具体的 epoch 值 (Optional.empty())。
                notifyEpochChange(Optional.empty());
            }
        }
    }

    /**
     * 获取此成员相对于组的当前状态。
     *
     * 应用场景: 外部组件或内部逻辑需要查询成员当前在消费组协议中所处的状态，
     * 例如，判断成员是否稳定、是否正在加入、是否已离开等。
     * 实现细节: 直接返回 {@link #state} 字段的值。
     * 设计考虑: 提供一个公开的访问器方法来获取成员的内部状态。
     *
     * @return 此成员相对于组的当前状态，定义在 {@link MemberState} 中。
     */
    public MemberState state() {
        // 实现细节: 返回成员的当前状态。
        return state;
    }

    /**
     * 获取成员从代理接收到的当前分配（主题 ID 和分区）。
     * 这是成员已成功协调的最后一个分配。
     *
     * 应用场景: 应用程序或内部组件需要获取消费者当前实际负责消费的分区集合。
     * 实现细节: 直接返回 {@link #currentAssignment} 字段的值。
     * {@link LocalAssignment} 封装了分区分配信息和相关的 epoch。
     * 设计考虑: 提供一个公开的访问器方法来获取成员已确认的分配。
     *
     * @return 成员的当前分配。
     */
    public LocalAssignment currentAssignment() {
        // 实现细节: 返回成员当前已成功协调的分配。
        return this.currentAssignment;
    }

    /**
     * 获取在目标分配中收到但尚未协调的主题 ID 集合。
     * 这些主题尚未协调的原因可能是：
     * 1. 主题名称不在元数据中（需要等待元数据更新）。
     * 2. 协调过程尚未完成（例如，回调正在执行，或正在等待偏移量提交）。
     * 如果当前活动分配中某个主题的分区集与目标分配中的不同，则认为该主题的协调尚未完成。
     *
     * 应用场景: 主要用于测试和内部状态检查，以了解哪些主题的分配仍在等待处理。
     * 实现细节: 调用 {@link #topicPartitionsAwaitingReconciliation()} 方法获取等待协调的主题及其分区，然后返回其键集合（即主题 ID 集合）。
     * 设计考虑: 提供一个便捷的方法来快速获取等待协调的主题列表。
     *
     * @return 等待协调的主题 ID 集合。
     *
     * 此方法对测试可见。
     */
    Set<Uuid> topicsAwaitingReconciliation() {
        // 实现细节: 调用 topicPartitionsAwaitingReconciliation() 获取等待协调的主题及其缺失的分区，
        // 然后返回这个 Map 的键集合，即等待协调的主题 ID 集合。
        return topicPartitionsAwaitingReconciliation().keySet();
    }

    /**
     * 获取在目标分配中收到但尚未协调的主题分区映射。
     * 这些主题分区尚未协调的原因可能是：
     * 1. 主题名称不在元数据中。
     * 2. 协调过程尚未完成。
     * 映射中的值是每个主题在目标分配中包含但当前已协调分配中缺失的分区集合。
     *
     * 应用场景: 主要用于测试和内部状态诊断，详细了解哪些主题的哪些分区仍在等待协调。
     * 实现细节:
     * 1. 如果没有目标分配 ({@code currentTargetAssignment == LocalAssignment.NONE})，则返回空映射。
     * 2. 如果没有当前已协调的分配 ({@code currentAssignment == LocalAssignment.NONE})，则目标分配中的所有分区都视为等待协调，返回目标分配的分区映射。
     * 3. 遍历目标分配中的每个主题及其分区：
     *    a. 获取当前已协调分配中对应主题的分区。
     *    b. 如果目标分区与已协调分区不完全相同，则计算出目标分配中存在但已协调分配中缺失的分区。
     *    c. 将主题 ID 和缺失的分区集合存入结果映射中。
     * 4. 返回不可修改的结果映射。
     * 设计考虑:
     * - 准确识别差异: 精确计算出目标分配与当前已协调分配之间的差异分区。
     * - 不可修改性: 返回的映射是不可修改的，防止外部意外更改内部状态。
     *
     * @return 等待协调的主题分区映射 (主题ID -> 目标分配中存在但当前分配中缺失的分区号集合)。
     *
     * 此方法对测试可见。
     */
    Map<Uuid, SortedSet<Integer>> topicPartitionsAwaitingReconciliation() {
        // 实现细节: 如果当前没有目标分配，则没有等待协调的分区。
        if (currentTargetAssignment == LocalAssignment.NONE) {
            return Collections.emptyMap(); // 返回一个空的不可修改的映射
        }
        // 实现细节: 如果当前没有任何已协调的分配，那么整个目标分配都在等待协调。
        if (currentAssignment == LocalAssignment.NONE) {
            return currentTargetAssignment.partitions; // 返回目标分配中的所有分区
        }
        // 实现细节: 创建一个 HashMap 来存储等待协调的主题及其分区。
        final Map<Uuid, SortedSet<Integer>> topicPartitionMap = new HashMap<>();
        // 实现细节: 遍历当前目标分配中的每一个主题ID和对应的目标分区集合。
        currentTargetAssignment.partitions.forEach((topicId, targetPartitions) -> {
            // 实现细节: 获取当前已成功协调的分配中，该主题ID对应的已协调分区集合。
            final SortedSet<Integer> reconciledPartitions = currentAssignment.partitions.get(topicId);
            // 实现细节: 如果目标分区集合与已协调分区集合不相等，说明该主题的分配尚未完全协调。
            if (!targetPartitions.equals(reconciledPartitions)) {
                // 实现细节: 创建一个新的 TreeSet，包含目标分配中的所有分区。
                final TreeSet<Integer> missingPartitions = new TreeSet<>(targetPartitions);
                if (reconciledPartitions != null) { // 如果该主题之前有已协调的分区
                    // 实现细节: 从 missingPartitions 中移除所有已协调的分区，剩下的就是目标分配中有但当前分配中没有的分区。
                    missingPartitions.removeAll(reconciledPartitions);
                }
                // 实现细节: 将主题ID和这些“缺失的”或“待添加的”分区集合放入结果映射中。
                topicPartitionMap.put(topicId, missingPartitions);
            }
        });
        // 实现细节: 返回一个不可修改的映射视图，以防止外部修改。
        return Collections.unmodifiableMap(topicPartitionMap);
    }

    /**
     * @return 如果当前正在进行协调。请注意，协调是由对 {@link #maybeReconcile()} 的调用触发的。此方法主要用于测试。
     * 应用场景：检查成员是否处于协调过程中，这对于测试和内部状态管理非常重要。
     * 实现细节：直接返回 {@code reconciliationInProgress} 字段的值。
     * 设计考虑：提供一个简单的方法来查询协调状态，方便外部调用者（主要是测试代码）了解内部状态。
     */
    boolean reconciliationInProgress() {
        // 返回 reconciliationInProgress 字段的当前值，该字段标记协调过程是否正在进行。
        return reconciliationInProgress;
    }

    /**
     * 注册一个新的监听器，每当成员状态发生变化，或者收到新的成员ID或epoch时，该监听器将被调用。
     * 应用场景：允许外部组件（如 ConsumerCoordinator）订阅成员状态的变更通知，以便做出相应的响应。
     * 实现细节：将传入的监听器添加到一个列表中。如果监听器为null，则抛出 IllegalArgumentException。
     * 设计考虑：采用观察者模式，解耦成员状态变化通知的发送方和接收方。
     *
     * @param listener 要调用的监听器。
     */
    public void registerStateListener(MemberStateListener listener) {
        // 检查传入的监听器是否为 null。
        if (listener == null) {
            // 如果监听器为 null，则抛出非法参数异常，因为状态更新监听器不能为空。
            throw new IllegalArgumentException("State updates listener cannot be null");
        }
        // 将监听器添加到 stateUpdatesListeners 列表中，以便在状态更新时通知它。
        this.stateUpdatesListeners.add(listener);
    }

    /**
     * 在 {@link Consumer} 的正常操作期间，请求管理器可能需要发送网络请求。
     * 实现可以通过在此处返回请求来返回其对网络 I/O 的需求 {@link NetworkClientDelegate.PollResult}。
     * 此方法在来自 {@link ConsumerNetworkThread 消费者的网络I/O线程} 的单线程上下文中调用。因此，此方法的实现中应该不需要同步保护。
     *
     * <p/>
     *
     * <em>注意</em>：对于成员资格管理器，此方法用于协调从组协调器收到的分配。
     * 它本身从不返回要发送的请求，因为心跳请求管理器负责协议的这方面。
     * 应用场景：在消费者网络线程的轮询周期中，给成员管理器一个机会去执行其周期性任务，主要是尝试进行分区分配的协调。
     * 实现细节：如果当前成员状态是 RECONCILING，则调用 maybeReconcile() 方法尝试进行协调。总是返回 NetworkClientDelegate.PollResult.EMPTY，因为成员管理器本身不直接发送网络请求（心跳由 HeartbeatRequestManager 处理）。
     * 设计考虑：将协调逻辑的触发点集成到消费者的主轮询循环中，确保协调操作能够及时执行。通过返回 EMPTY 表明它不直接产生网络请求，符合其职责分离的设计。
     *
     * @param currentTimeMs 调用该方法时的当前系统时间；用于确定是否应执行时间敏感的操作。
     */
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        // 检查当前成员状态是否为 RECONCILING（正在协调）。
        if (state == MemberState.RECONCILING) {
            // 如果是，则调用 maybeReconcile 方法尝试进行协调操作。
            maybeReconcile();
        }
        // 返回一个空的 PollResult，表示此管理器在此轮询中没有生成任何网络请求。
        return NetworkClientDelegate.PollResult.EMPTY;
    }

    // 主要用于测试
    /**
     * 获取已注册的状态监听器列表。
     * 应用场景：主要用于测试，验证监听器是否已正确注册。
     * 实现细节：返回一个不可修改的 stateUpdatesListeners 列表副本，以防止外部修改。
     * 设计考虑：提供对内部状态的只读访问，便于测试，同时通过返回不可修改列表来保证内部状态的安全性。
     * @return 不可修改的成员状态监听器列表。
     */
    List<MemberStateListener> stateListeners() {
        // 返回 stateUpdatesListeners 列表的不可修改视图，以防止外部直接修改列表内容。
        return unmodifiableList(stateUpdatesListeners);
    }

    /**
     * 一个数据结构，用于表示消费者组成员的当前分配和当前目标分配。
     *
     * 除了分配的分区外，它还包含一个本地epoch，每当分配发生变化时，该epoch就会增加，
     * 以确保具有相同分区但不同本地epoch的两个分配不被视为相等。
     * 应用场景：封装成员的本地分区分配信息，包括分区集合和用于版本控制的本地epoch。
     * 设计考虑：将分区分配和其版本（本地epoch）绑定在一起，有助于检测分配的变化，即使分区集合本身没有改变（例如，只是epoch增加了）。
     */
    public static class LocalAssignment {

        /**
         * 表示没有分配或无效epoch的常量值。
         * 应用场景：用于初始化或标记一个无效的、空的或未定义的分配状态。
         * 设计考虑：提供一个明确的常量来表示“无epoch”状态，避免使用魔法数字。
         */
        public static final long NONE_EPOCH = -1;

        /**
         * 表示没有分配的 LocalAssignment 实例。
         * 应用场景：作为默认的或初始的分配状态，表示成员当前没有任何分区分配。
         * 设计考虑：提供一个静态的、不可变的“无分配”实例，方便使用并减少对象创建。
         */
        public static final LocalAssignment NONE = new LocalAssignment(NONE_EPOCH, Collections.emptyMap());

        /**
         * 本地epoch，每当分配发生变化时递增。
         * 应用场景：用于跟踪本地分配的版本。当服务器下发新的分配或者本地分配因某些原因（如订阅变化）更新时，此epoch会增加。
         * 设计考虑：通过epoch来区分不同的分配版本，即使分配的分区集合可能相同。
         */
        public final long localEpoch;

        /**
         * 分配给成员的分区，按主题ID（Uuid）映射到该主题的分区号集合（SortedSet<Integer>）。
         * 应用场景：存储成员实际分配到的分区。使用Map结构可以快速按主题ID查找分区，SortedSet保证分区号有序。
         * 设计考虑：选择合适的数据结构来存储分区信息，既要方便查找，也要保证分区顺序的确定性（如果需要）。
         */
        public final Map<Uuid, SortedSet<Integer>> partitions;

        /**
         * LocalAssignment 的构造函数。
         * 应用场景：创建一个新的 LocalAssignment 实例，表示一个具体的分区分配和其本地epoch。
         * 实现细节：初始化 localEpoch 和 partitions 字段。如果 localEpoch 是 NONE_EPOCH 但分区列表不为空，则抛出异常，因为有分区分配时必须有一个有效的epoch。
         * 设计考虑：确保 LocalAssignment 实例状态的一致性，有分区就必须有有效的epoch。
         *
         * @param localEpoch 此分配的本地epoch。
         * @param partitions 分配的分区，按主题ID映射到分区号集合。
         */
        public LocalAssignment(long localEpoch, Map<Uuid, SortedSet<Integer>> partitions) {
            // 初始化本地 epoch。
            this.localEpoch = localEpoch;
            // 初始化分区映射。
            this.partitions = partitions;
            // 检查一致性：如果 epoch 是 NONE_EPOCH（表示无分配或无效），但分区列表不为空。
            if (localEpoch == NONE_EPOCH && !partitions.isEmpty()) {
                // 抛出非法参数异常，因为如果存在分区，则本地 epoch 必须被设置。
                throw new IllegalArgumentException("Local epoch must be set if there are partitions");
            }
        }

        /**
         * LocalAssignment 的构造函数，接受一个 TopicIdPartition 集合。
         * 应用场景：从一个 TopicIdPartition 集合方便地创建 LocalAssignment 实例。TopicIdPartition 同时包含主题ID和分区信息。
         * 实现细节：初始化 localEpoch。创建一个新的 HashMap 用于存储分区。遍历输入的 TopicIdPartition 集合，将其转换为期望的 Map<Uuid, SortedSet<Integer>> 格式。同样，如果 localEpoch 是 NONE_EPOCH 但 TopicIdPartition 集合不为空，则抛出异常。
         * 设计考虑：提供多种构造方式以适应不同的输入数据格式，提高易用性。
         *
         * @param localEpoch 此分配的本地epoch。
         * @param topicIdPartitions 分配的主题ID分区集合。
         */
        public LocalAssignment(long localEpoch, SortedSet<TopicIdPartition> topicIdPartitions) {
            // 初始化本地 epoch。
            this.localEpoch = localEpoch;
            // 初始化分区映射为一个新的 HashMap。
            this.partitions = new HashMap<>();
            // 检查一致性：如果 epoch 是 NONE_EPOCH，但 topicIdPartitions 列表不为空。
            if (localEpoch == NONE_EPOCH && !topicIdPartitions.isEmpty()) {
                // 抛出非法参数异常，因为如果存在分区，则本地 epoch 必须被设置。
                throw new IllegalArgumentException("Local epoch must be set if there are partitions");
            }
            // 遍历输入的 topicIdPartitions 集合。
            topicIdPartitions.forEach(topicIdPartition -> {
                // 获取当前 TopicIdPartition 的主题ID。
                Uuid topicId = topicIdPartition.topicId();
                // 将分区添加到 partitions 映射中。如果主题ID尚不存在，则为其创建一个新的 TreeSet。
                partitions.computeIfAbsent(topicId, k -> new TreeSet<>()).add(topicIdPartition.partition());
            });
        }

        public String toString() {
            return "LocalAssignment{" +
                    "localEpoch=" + localEpoch +
                    ", partitions=" + partitions +
                    '}';
        }

        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final LocalAssignment that = (LocalAssignment) o;
            return localEpoch == that.localEpoch && Objects.equals(partitions, that.partitions);
        }

        public int hashCode() {
            return Objects.hash(localEpoch, partitions);
        }

        /**
         * 检查此 LocalAssignment 是否表示没有分配 (NONE)。
         * 应用场景：判断当前分配是否为空或无效。
         * 实现细节：通过比较 localEpoch 是否等于 NONE_EPOCH 来判断。
         * 设计考虑：提供一个清晰的方法来检查“无分配”状态。
         *
         * @return 如果 localEpoch 等于 NONE_EPOCH，则返回 true；否则返回 false。
         */
        public boolean isNone() {
            // 如果本地 epoch 等于 NONE_EPOCH 常量，则表示这是一个“无分配”的实例。
            return localEpoch == NONE_EPOCH;
        }

        /**
         * 使用新的分区分配更新当前的 LocalAssignment。
         * 如果新的分配与当前分配相同（仅比较分区，不比较epoch），则返回 Optional.empty()。
         * 否则，创建一个新的 LocalAssignment，其 localEpoch 比当前epoch大1，并包含新的分区分配。
         * 应用场景：当收到新的目标分配时，用于更新成员的本地分配。如果分配没有实际变化，则不创建新对象。
         * 实现细节：首先检查当前是否存在分配 (localEpoch != NONE_EPOCH)。如果存在且新分配的分区与当前分区相同，则返回空 Optional。否则，将 localEpoch 加1，并用新的分区创建一个新的 LocalAssignment 实例返回。
         * 设计考虑：通过比较分区内容来避免不必要的epoch增加和对象创建。返回 Optional<LocalAssignment> 明确表示更新可能不会发生（如果分配未变）。
         *
         * @param assignment 新的分区分配，格式为 Map<Uuid, SortedSet<Integer>>。
         * @return 如果分配有变化，则返回包含更新后的 LocalAssignment 的 Optional；如果分配无变化，则返回 Optional.empty()。
         */
        Optional<LocalAssignment> updateWith(Map<Uuid, SortedSet<Integer>> assignment) {
            // 检查当前是否存在有效的分配（即 localEpoch 不是 NONE_EPOCH）。
            if (localEpoch != NONE_EPOCH) {
                // 如果当前分配有效，并且新的分配内容与当前分区内容相同。
                if (assignment.equals(partitions)) {
                    // 返回空的 Optional，表示分配没有变化，不需要更新。
                    return Optional.empty();
                }
            }

            // 如果分配有变化，或者当前没有有效分配，则增加本地 epoch。
            long nextLocalEpoch = localEpoch + 1;
            // 创建一个新的 LocalAssignment 实例，使用新的 epoch 和新的分配内容。
            return Optional.of(new LocalAssignment(nextLocalEpoch, assignment));
        }
    }

    /*
     * 主要用于测试。
     */
    /**
     * 检查订阅是否已更新。
     * 应用场景：主要用于测试，判断成员的订阅信息自上次检查以来是否发生了变化。
     * 实现细节：返回 {@code subscriptionUpdated} AtomicBoolean 字段的当前值。
     * 设计考虑：提供一个线程安全的方式来查询订阅更新状态，主要服务于测试场景。
     *
     * @return 如果订阅已更新，则返回 true；否则返回 false。
     */
    boolean subscriptionUpdated() {
        // 返回 subscriptionUpdated (一个 AtomicBoolean) 的当前值。
        return subscriptionUpdated.get();
    }
}
