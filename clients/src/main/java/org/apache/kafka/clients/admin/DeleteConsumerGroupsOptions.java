/*
 * 授权给Apache软件基金会(ASF)的一个或多个贡献者许可协议。
 * 有关版权所有权的其他信息，请参见随本作品分发的NOTICE文件。
 * ASF根据Apache许可证2.0版（"许可证"）将本文件授权给您；
 * 除非符合许可证，否则您不得使用此文件。
 * 您可以在以下位置获取许可证副本：
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * 除非适用法律要求或书面同意，否则根据许可证分发的软件是基于
 * "按原样"提供的，没有任何明示或暗示的担保或条件。
 * 有关许可证下的特定语言管理权限和限制，请参阅许可证。
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;

/**
 * 用于配置{@link Admin#deleteConsumerGroups(Collection)}调用的选项类。
 * 该类提供了删除消费者组操作的配置选项，包括：
 * 1. 继承自AbstractOptions的通用配置，如超时设置
 * 2. 支持批量删除多个消费者组
 * 3. 删除操作会同时清理消费者组的元数据和组内成员信息
 *
 * 注意：该类的API仍在演进中，详细信息请参见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DeleteConsumerGroupsOptions extends AbstractOptions<DeleteConsumerGroupsOptions> {

}
