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
 *
 * 上述为Apache License Version 2.0许可证声明
 */
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Utils;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Kafka管理客户端工具类，用于处理ACL（访问控制列表）相关的操作。
 * 该类提供了验证和过滤ACL操作的功能，确保只返回有效的ACL操作集合。
 */
public final class AdminUtils {

    /**
     * 私有构造函数，防止实例化。
     * 由于该类只包含静态工具方法，不需要创建实例。
     */
    private AdminUtils() {}

    /**
     * 从授权操作的32位字段中提取有效的ACL操作集合。
     * 
     * @param authorizedOperations 包含授权操作信息的32位整数字段
     * @return 有效的ACL操作集合。如果授权操作被省略，则返回null
     *
     * 实现细节：
     * 1. 首先检查授权操作是否被省略，如果是则返回null
     * 2. 使用Utils.from32BitField将32位整数转换为比特位的集合
     * 3. 将每个比特位映射为对应的AclOperation枚举值
     * 4. 过滤掉无效的操作类型（UNKNOWN、ALL和ANY）
     * 5. 最终返回一个包含所有有效ACL操作的Set集合
     */
    public static Set<AclOperation> validAclOperations(final int authorizedOperations) {
        if (authorizedOperations == MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED) {
            return null;
        }
        return Utils.from32BitField(authorizedOperations)
            .stream()
            .map(AclOperation::fromCode)
            .filter(operation -> operation != AclOperation.UNKNOWN
                && operation != AclOperation.ALL
                && operation != AclOperation.ANY)
            .collect(Collectors.toSet());
    }
}
