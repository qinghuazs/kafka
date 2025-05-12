/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.network.RequestChannel
import kafka.utils.Logging
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult
import org.apache.kafka.common.message._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.security.authorizer.AuthorizerUtils
import org.apache.kafka.server.authorizer._

import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional

/**
 * ACL请求处理逻辑类
 * 负责处理Kafka集群中的ACL相关请求，包括描述、创建和删除ACL
 * 
 * @param authHelper 认证助手，用于处理认证相关操作
 * @param authorizer 授权器选项，用于执行实际的ACL操作
 * @param requestHelper 请求处理助手，用于处理请求响应
 * @param name 实例名称
 * @param config Kafka配置
 */
class AclApis(authHelper: AuthHelper,
              authorizer: Option[Authorizer],
              requestHelper: RequestHandlerHelper,
              name: String,
              config: KafkaConfig) extends Logging {
  // 设置日志标识符
  this.logIdent = "[AclApis-%s-%s] ".format(name, config.nodeId)
  
  // 创建ACL修改延迟处理器，用于异步处理ACL修改请求
  private val alterAclsPurgatory =
      new DelayedFuturePurgatory(purgatoryName = "AlterAcls", brokerId = config.nodeId)

  /**
   * 检查ACL处理器是否已关闭
   * @return 如果延迟处理器已关闭返回true
   */
  def isClosed: Boolean = alterAclsPurgatory.isShutdown

  /**
   * 关闭ACL处理器
   * 关闭延迟处理器并释放资源
   */
  def close(): Unit = alterAclsPurgatory.shutdown()

  /**
   * 处理描述ACL请求
   * 获取指定资源的ACL信息
   * 
   * @param request 请求通道请求
   * @return 完成的Future
   */
  def handleDescribeAcls(request: RequestChannel.Request): CompletableFuture[Unit] = {
    // 验证请求者是否有权限执行DESCRIBE操作
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    // 获取描述ACL请求体
    val describeAclsRequest = request.body[DescribeAclsRequest]
    
    // 根据授权器是否配置进行不同处理
    authorizer match {
      case None =>
        // 如果未配置授权器，返回安全禁用错误
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new DescribeAclsResponse(new DescribeAclsResponseData()
            .setErrorCode(Errors.SECURITY_DISABLED.code)
            .setErrorMessage("No Authorizer is configured on the broker")
            .setThrottleTimeMs(requestThrottleMs),
          describeAclsRequest.version))
      case Some(auth) =>
        // 如果配置了授权器，获取ACL过滤器并返回匹配的资源
        val filter = describeAclsRequest.filter
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new DescribeAclsResponse(new DescribeAclsResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setResources(DescribeAclsResponse.aclsResources(auth.acls(filter))),
          describeAclsRequest.version))
    }
    CompletableFuture.completedFuture[Unit](())
  }

  /**
   * 处理创建ACL请求
   * 创建新的ACL规则
   * 
   * @param request 请求通道请求
   * @return 完成的Future
   */
  def handleCreateAcls(request: RequestChannel.Request): CompletableFuture[Unit] = {
    // 验证请求者是否有权限执行ALTER操作
    authHelper.authorizeClusterOperation(request, ALTER)
    // 获取创建ACL请求体
    val createAclsRequest = request.body[CreateAclsRequest]

    // 根据授权器是否配置进行不同处理
    authorizer match {
      case None => 
        // 如果未配置授权器，返回安全禁用异常
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          createAclsRequest.getErrorResponse(requestThrottleMs,
            new SecurityDisabledException("No Authorizer is configured.")))
        CompletableFuture.completedFuture[Unit](())
      case Some(auth) =>
        // 获取所有ACL绑定
        val allBindings = createAclsRequest.aclCreations.asScala.map(CreateAclsRequest.aclBinding)
        // 存储错误结果
        val errorResults = mutable.Map[AclBinding, AclCreateResult]()
        // 存储有效的绑定
        val validBindings = new ArrayBuffer[AclBinding]
        
        // 验证每个ACL绑定
        allBindings.foreach { acl =>
          val resource = acl.pattern
          // 检查资源类型和名称的有效性
          val throwable = if (resource.resourceType == ResourceType.CLUSTER && !AuthorizerUtils.isClusterResource(resource.name))
              new InvalidRequestException("The only valid name for the CLUSTER resource is " + CLUSTER_NAME)
          else if (resource.name.isEmpty)
            new InvalidRequestException("Invalid empty resource name")
          else
            null
          if (throwable != null) {
            // 记录失败的ACL添加
            debug(s"Failed to add acl $acl to $resource", throwable)
            errorResults(acl) = new AclCreateResult(throwable)
          } else
            // 添加有效的绑定
            validBindings += acl
        }

        // 创建Future用于异步处理结果
        val future = new CompletableFuture[util.List[AclCreationResult]]()
        // 创建ACL并获取结果
        val createResults = auth.createAcls(request.context, validBindings.asJava).asScala.map(_.toCompletableFuture)

        // 定义发送响应的回调函数
        def sendResponseCallback(): Unit = {
          val aclCreationResults = allBindings.map { acl =>
            val result = errorResults.getOrElse(acl, createResults(validBindings.indexOf(acl)).get)
            val creationResult = new AclCreationResult()
            // 处理异常情况
            result.exception.toScala.foreach { throwable =>
              val apiError = ApiError.fromThrowable(throwable)
              creationResult
                .setErrorCode(apiError.error.code)
                .setErrorMessage(apiError.message)
            }
            creationResult
          }
          future.complete(aclCreationResults.asJava)
        }
        
        // 尝试完成或监视创建结果
        alterAclsPurgatory.tryCompleteElseWatch(config.connectionsMaxIdleMs, createResults, sendResponseCallback)

        // 处理最终结果并发送响应
        future.thenApply[Unit] { aclCreationResults =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new CreateAclsResponse(new CreateAclsResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setResults(aclCreationResults)))
        }
    }
  }

  /**
   * 处理删除ACL请求
   * 删除指定的ACL规则
   * 
   * @param request 请求通道请求
   * @return 完成的Future
   */
  def handleDeleteAcls(request: RequestChannel.Request): CompletableFuture[Unit] = {
    // 验证请求者是否有权限执行ALTER操作
    authHelper.authorizeClusterOperation(request, ALTER)
    // 获取删除ACL请求体
    val deleteAclsRequest = request.body[DeleteAclsRequest]
    
    // 根据授权器是否配置进行不同处理
    authorizer match {
      case None =>
        // 如果未配置授权器，返回安全禁用异常
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          deleteAclsRequest.getErrorResponse(requestThrottleMs,
            new SecurityDisabledException("No Authorizer is configured.")))
        CompletableFuture.completedFuture[Unit](())
      case Some(auth) =>
        // 创建Future用于异步处理结果
        val future = new CompletableFuture[util.List[DeleteAclsFilterResult]]()
        // 删除ACL并获取结果
        val deleteResults = auth.deleteAcls(request.context, deleteAclsRequest.filters)
          .asScala.map(_.toCompletableFuture).toList

        // 定义发送响应的回调函数
        def sendResponseCallback(): Unit = {
          val filterResults = deleteResults.map(_.get).map(DeleteAclsResponse.filterResult).asJava
          future.complete(filterResults)
        }

        // 尝试完成或监视删除结果
        alterAclsPurgatory.tryCompleteElseWatch(config.connectionsMaxIdleMs, deleteResults, sendResponseCallback)
        
        // 处理最终结果并发送响应
        future.thenApply[Unit] { filterResults =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new DeleteAclsResponse(
              new DeleteAclsResponseData()
                .setThrottleTimeMs(requestThrottleMs)
                .setFilterResults(filterResults),
              deleteAclsRequest.version))
        }
    }
  }
}
