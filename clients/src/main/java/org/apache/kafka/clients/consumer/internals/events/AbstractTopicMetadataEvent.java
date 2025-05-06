/*
 * 授权给Apache软件基金会(ASF)的许可证，基于一个或多个贡献者许可协议。
 * 查看NOTICE文件了解版权所有权的更多信息。
 * ASF基于Apache许可证2.0版本授权给您使用本文件，
 * 除非符合许可证规定，否则您不能使用本文件。
 * 您可以在以下位置获取许可证副本：
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * 除非适用法律要求或书面同意，基于许可证分发的软件是基于
 * "按原样"提供的基础上，没有任何形式的保证或条件。
 * 请查看许可证了解具体的权限和限制。
 */
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * 主题元数据事件的抽象基类，用于处理Kafka消费者客户端中的主题元数据相关操作。
 * 该类继承自CompletableApplicationEvent，并使用Map<String, List<PartitionInfo>>作为泛型参数，
 * 表示操作完成后返回的结果类型是一个映射，其中键为主题名称，值为该主题的分区信息列表。
 * 
 * 设计考虑：
 * 1. 通过抽象类设计模式，为不同类型的主题元数据事件提供统一的基础实现
 * 2. 使用泛型来约束返回结果的类型，确保类型安全
 * 3. 继承CompletableApplicationEvent以支持异步操作完成的通知机制
 */
public abstract class AbstractTopicMetadataEvent extends CompletableApplicationEvent<Map<String, List<PartitionInfo>>> {

    /**
     * 构造函数，用于创建主题元数据事件实例
     * 
     * @param type 事件类型，用于标识具体的元数据事件类型
     * @param deadlineMs 事件的截止时间（以毫秒为单位），表示事件必须在该时间之前完成
     */
    protected AbstractTopicMetadataEvent(final Type type, final long deadlineMs) {
        super(type, deadlineMs);
    }

    /**
     * 重写父类方法，指示该事件是否需要订阅元数据
     * 
     * @return 始终返回true，因为主题元数据事件需要访问订阅信息来完成元数据的获取和更新
     */
    @Override
    public boolean requireSubscriptionMetadata() {
        return true;
    }
}
