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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link Admin#describeUserScramCredentials()} 调用的结果类。
 * 该类用于获取Kafka用户的SCRAM（Salted Challenge Response Authentication Mechanism）凭证信息。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeUserScramCredentialsResult {
    // 存储SCRAM凭证描述响应数据的Future
    private final KafkaFuture<DescribeUserScramCredentialsResponseData> dataFuture;

    /**
     * 包级私有构造函数
     *
     * @param dataFuture 包含调用响应数据的Future
     */
    DescribeUserScramCredentialsResult(KafkaFuture<DescribeUserScramCredentialsResponseData> dataFuture) {
        // 确保dataFuture不为null，否则抛出NullPointerException
        this.dataFuture = Objects.requireNonNull(dataFuture);
    }

    /**
     * 获取所有已描述用户的凭证信息
     * 
     * @return 返回一个Future，包含所有用户的凭证描述信息映射。只有当所有用户的描述都成功完成时，Future才会成功完成。
     *         映射的键为用户名，与{@link #users()}返回的列表内容一致。
     */
    public KafkaFuture<Map<String, UserScramCredentialsDescription>> all() {
        // 创建返回结果的Future实现
        final KafkaFutureImpl<Map<String, UserScramCredentialsDescription>> retval = new KafkaFutureImpl<>();
        
        // 当数据Future完成时执行回调
        dataFuture.whenComplete((data, throwable) -> {
            if (throwable != null) {
                // 如果发生异常，使用该异常完成返回的Future
                retval.completeExceptionally(throwable);
            } else {
                // 检查每个用户的描述是否成功
                // 成功的用户描述必须具有NONE或RESOURCE_NOT_FOUND错误码
                // RESOURCE_NOT_FOUND表示客户端请求描述的用户不存在，这样的用户不会出现在返回的映射中
                Optional<DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult> optionalFirstFailedDescribe =
                        data.results().stream()
                            .filter(result -> result.errorCode() != Errors.NONE.code() && 
                                            result.errorCode() != Errors.RESOURCE_NOT_FOUND.code())
                            .findFirst();
                
                if (optionalFirstFailedDescribe.isPresent()) {
                    // 如果存在失败的描述，使用第一个失败的错误信息完成Future
                    retval.completeExceptionally(Errors.forCode(optionalFirstFailedDescribe.get().errorCode())
                            .exception(optionalFirstFailedDescribe.get().errorMessage()));
                } else {
                    // 创建结果映射并填充数据
                    Map<String, UserScramCredentialsDescription> retvalMap = new HashMap<>();
                    data.results().stream().forEach(userResult ->
                            retvalMap.put(userResult.user(), 
                                    new UserScramCredentialsDescription(userResult.user(),
                                            getScramCredentialInfosFor(userResult))));
                    // 成功完成Future
                    retval.complete(retvalMap);
                }
            }
        });
        return retval;
    }

    /**
     * 获取满足请求条件且至少有一个凭证的用户列表
     * 
     * @return 返回一个Future，包含用户名列表。如果用户没有执行describe操作的权限，Future将异常完成。
     *         返回的列表不包含不存在或没有凭证的用户。如果请求描述的用户列表中没有任何用户存在或有凭证，
     *         将返回空列表。返回的列表包含有凭证但无法描述的用户。
     */
    public KafkaFuture<List<String>> users() {
        // 创建返回结果的Future实现
        final KafkaFutureImpl<List<String>> retval = new KafkaFutureImpl<>();
        
        // 当数据Future完成时执行回调
        dataFuture.whenComplete((data, throwable) -> {
            if (throwable != null) {
                // 如果发生异常，使用该异常完成返回的Future
                retval.completeExceptionally(throwable);
            } else {
                // 过滤出非RESOURCE_NOT_FOUND的结果，获取用户名列表
                retval.complete(data.results().stream()
                        .filter(result -> result.errorCode() != Errors.RESOURCE_NOT_FOUND.code())
                        .map(result -> result.user())
                        .collect(Collectors.toList()));
            }
        });
        return retval;
    }

    /**
     * 获取指定用户的凭证描述信息
     * 
     * @param userName 要描述的用户名
     * @return 返回一个Future，包含指定用户的凭证描述信息。如果{@link #users()}返回的Future异常完成，
     *         该Future也会异常完成。如果指定用户不存在于描述的用户列表中，Future将以
     *         {@link org.apache.kafka.common.errors.ResourceNotFoundException}异常完成。
     */
    public KafkaFuture<UserScramCredentialsDescription> description(String userName) {
        // 创建返回结果的Future实现
        final KafkaFutureImpl<UserScramCredentialsDescription> retval = new KafkaFutureImpl<>();
        
        // 当数据Future完成时执行回调
        dataFuture.whenComplete((data, throwable) -> {
            if (throwable != null) {
                // 如果发生异常，使用该异常完成返回的Future
                retval.completeExceptionally(throwable);
            } else {
                // 查找指定用户的结果
                Optional<DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult> optionalUserResult =
                        data.results().stream()
                            .filter(result -> result.user().equals(userName))
                            .findFirst();
                
                if (optionalUserResult.isEmpty()) {
                    // 如果用户不存在，返回ResourceNotFoundException
                    retval.completeExceptionally(new ResourceNotFoundException("No such user: " + userName));
                } else {
                    DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult userResult = optionalUserResult.get();
                    if (userResult.errorCode() != Errors.NONE.code()) {
                        // 如果有错误（包括RESOURCE_NOT_FOUND），返回对应的异常
                        retval.completeExceptionally(Errors.forCode(userResult.errorCode())
                                .exception(userResult.errorMessage()));
                    } else {
                        // 成功完成Future，返回用户凭证描述信息
                        retval.complete(new UserScramCredentialsDescription(userResult.user(), 
                                getScramCredentialInfosFor(userResult)));
                    }
                }
            }
        });
        return retval;
    }

    /**
     * 从用户结果中获取SCRAM凭证信息列表
     * 
     * @param userResult 用户SCRAM凭证结果
     * @return 返回用户的SCRAM凭证信息列表
     */
    private static List<ScramCredentialInfo> getScramCredentialInfosFor(
            DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult userResult) {
        // 将凭证信息转换为ScramCredentialInfo对象列表
        return userResult.credentialInfos().stream()
                .map(c -> new ScramCredentialInfo(ScramMechanism.fromType(c.mechanism()), c.iterations()))
                .collect(Collectors.toList());
    }
}
