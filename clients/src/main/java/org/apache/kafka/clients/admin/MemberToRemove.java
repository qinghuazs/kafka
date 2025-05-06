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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Objects;

/**
 * 用于描述要从Kafka消费者组中移除的成员信息的数据结构。
 * 
 * 应用场景：
 * 1. 在消费者组管理中，当需要移除特定消费者组成员时使用
 * 2. 作为AdminClient.removeMembersFromConsumerGroup()方法的参数
 * 3. 支持批量移除消费者组成员的操作
 */
public class MemberToRemove {
    /**
     * 消费者组成员的实例ID
     * 这是一个持久性的标识符，在消费者重启后仍然保持不变
     * 用于唯一标识消费者组中的特定成员实例
     */
    private final String groupInstanceId;

    /**
     * 创建一个新的MemberToRemove实例
     * 
     * @param groupInstanceId 要移除的消费者组成员的实例ID
     */
    public MemberToRemove(String groupInstanceId) {
        this.groupInstanceId = groupInstanceId;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MemberToRemove) {
            MemberToRemove otherMember = (MemberToRemove) o;
            return this.groupInstanceId.equals(otherMember.groupInstanceId);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupInstanceId);
    }

    /**
     * 将MemberToRemove转换为MemberIdentity
     * 
     * 实现细节：
     * 1. 创建新的MemberIdentity实例
     * 2. 设置groupInstanceId作为成员标识
     * 3. 设置memberId为UNKNOWN_MEMBER_ID，因为实际的memberId在服务器端进行解析
     * 
     * @return 包含成员身份信息的MemberIdentity对象
     */
    MemberIdentity toMemberIdentity() {
        return new MemberIdentity()
            .setGroupInstanceId(groupInstanceId)
            .setMemberId(JoinGroupRequest.UNKNOWN_MEMBER_ID);
    }

    /**
     * 获取消费者组成员的实例ID
     * 
     * @return 消费者组成员的实例ID
     */
    public String groupInstanceId() {
        return groupInstanceId;
    }
}
