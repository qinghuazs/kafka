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
import org.apache.kafka.common.errors.ApiException;

import java.util.Optional;

@InterfaceStability.Evolving // 标记该接口尚在演进中，未来可能会发生变化
public class AclCreateResult { // ACL创建结果类，封装了创建ACL操作的结果。
    // 应用场景：当Kafka集群启用了ACL授权后，任何创建ACL的请求都会返回此对象，表明操作是否成功以及失败原因。
    // 设计考虑：通过封装结果，可以清晰地向上层调用者传递操作状态，便于进行错误处理和流程控制。

    /**
     * 表示成功的ACL创建结果的静态常量。
     * 应用场景：当ACL创建操作成功时，可以直接返回此预定义实例，避免重复创建对象。
     * 设计考虑：使用静态常量可以提高性能并减少内存占用。
     */
    public static final AclCreateResult SUCCESS = new AclCreateResult(); // 定义一个静态常量SUCCESS，表示成功的ACL创建结果，初始化为一个没有异常的AclCreateResult实例。

    /**
     * ACL创建过程中可能发生的API异常。
     * 如果为null，表示创建成功。
     * 设计考虑：使用final修饰，确保exception在对象创建后不可变，增强了对象的线程安全性。
     */
    private final ApiException exception; // 定义一个私有的、final的ApiException类型的字段，用于存储ACL创建过程中可能发生的异常。

    /**
     * 私有构造函数，用于创建表示成功的AclCreateResult实例。
     * 内部调用另一个构造函数，并传递null作为异常参数。
     * 应用场景：主要由SUCCESS常量初始化时调用。
     * 设计考虑：将无参构造函数设为私有，可以控制对象的创建方式，确保只有预期的成功实例被创建。
     */
    private AclCreateResult() { // 私有构造函数，用于创建成功的AclCreateResult实例。
        this(null); // 调用另一个构造函数，传入null表示没有异常。
    }

    /**
     * 公共构造函数，用于创建AclCreateResult实例，可以指定一个异常。
     * @param exception ACL创建过程中发生的API异常，如果为null，表示成功。
     * 应用场景：当ACL创建操作失败时，调用此构造函数并传入具体的异常信息。
     * 设计考虑：允许传入null异常，统一了成功和失败情况下的对象创建逻辑。
     */
    public AclCreateResult(ApiException exception) { // 公共构造函数，接收一个ApiException作为参数。
        this.exception = exception; // 将传入的exception赋值给成员变量this.exception。
    }

    /**
     * 返回创建过程中发生的任何异常。如果异常为空，则表示请求已成功。
     * @return 一个包含ApiException的Optional对象，如果创建成功则为空Optional。
     * 应用场景：调用者通过此方法检查ACL创建操作是否成功，并获取失败时的具体异常信息。
     * 设计考虑：使用Optional可以更优雅地处理可能为null的异常对象，避免空指针异常。
     */
    public Optional<ApiException> exception() { // 返回创建过程中可能发生的异常。
        // 检查成员变量exception是否为null
        return exception == null ? Optional.empty() : Optional.of(exception); // 如果exception为null，则返回一个空的Optional对象；否则，返回包含该exception的Optional对象。
    }
}
