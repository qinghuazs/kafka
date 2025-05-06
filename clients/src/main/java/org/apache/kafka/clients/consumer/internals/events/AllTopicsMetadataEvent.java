/*
 * 授权给Apache软件基金会(ASF)，基于一个或多个贡献者许可协议。
 * 查看NOTICE文件了解更多关于版权所有权的信息。
 * ASF基于Apache许可证2.0版本授权该文件给您（"许可证"）；
 * 除非符合许可证，否则您不能使用此文件。
 * 您可以在以下位置获取许可证副本：
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * 除非适用法律要求或书面同意，否则根据许可证分发的软件是基于
 * "按原样"提供的，没有任何明示或暗示的担保或条件。
 * 请查看许可证了解具体的许可证语言和限制条件。
 */
package org.apache.kafka.clients.consumer.internals.events;

/**
 * 全部主题元数据事件类，用于获取Kafka集群中所有主题的元数据信息。
 * 该事件继承自AbstractTopicMetadataEvent，是Kafka消费者客户端中用于
 * 异步获取所有主题元数据的事件实现。
 *
 * 主要应用场景：
 * 1. 消费者初始化时需要获取集群中所有主题信息
 * 2. 消费者使用正则表达式订阅主题时，需要获取所有主题来匹配模式
 * 3. 定期刷新元数据以保持与集群状态的同步
 *
 * 工作机制：
 * - 通过事件类型Type.ALL_TOPICS_METADATA标识这是一个获取所有主题元数据的请求
 * - 使用deadlineMs参数控制元数据请求的超时时间，确保请求不会无限期等待
 * - 继承自AbstractTopicMetadataEvent，复用了通用的元数据处理逻辑
 */
public class AllTopicsMetadataEvent extends AbstractTopicMetadataEvent {

    /**
     * 构造一个新的全部主题元数据事件实例
     *
     * @param deadlineMs 事件处理的截止时间（以毫秒为单位）
     *                   用于控制元数据请求的超时时间，防止请求长时间阻塞
     */
    public AllTopicsMetadataEvent(final long deadlineMs) {
        super(Type.ALL_TOPICS_METADATA, deadlineMs);
    }
}
