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

package kafka.server.logger;

import kafka.utils.Log4jController;

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.config.LogLevelConfig;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.protocol.Errors;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER_LOGGER;

/**
 * Manages runtimes changes to slf4j settings.
 * 管理slf4j日志配置的运行时动态修改。
 * 该类负责处理Kafka Broker的日志级别动态调整功能，包括验证和执行日志配置变更。
 * 允许管理员在运行时修改Kafka组件的日志级别，而无需重启服务。这使得在生产环境中进行问题诊断和调试变得更加方便。
 */
public class RuntimeLoggerManager {
    
    // 有效的日志级别字符串，用于错误提示
    static final String VALID_LOG_LEVELS_STRING;

    static {
        // 初始化有效日志级别字符串，按字母顺序排序
        ArrayList<String> logLevels = new ArrayList<>(LogLevelConfig.VALID_LOG_LEVELS);
        logLevels.sort(String::compareTo);
        // 拼接为逗号分隔的字符串
        VALID_LOG_LEVELS_STRING = String.join(", ", logLevels);
    }

    // 当前节点ID
    private final int nodeId;
    // 用于记录操作日志的Logger实例
    private final Logger log;

    /**
     * 构造函数
     * @param nodeId 当前broker节点的ID
     * @param log 用于记录操作的Logger实例
     */
    public RuntimeLoggerManager(int nodeId,  Logger log) {
        this.nodeId = nodeId;
        this.log = log;
    }

    /**
     * 应用日志配置变更
     * @param authorizedForClusterResource 是否有集群资源的操作权限
     * @param validateOnly 是否仅进行验证而不实际执行变更
     * @param resource 要修改的配置资源
     */
    public void applyChangesForResource(
        boolean authorizedForClusterResource,
        boolean validateOnly,
        AlterConfigsResource resource
    ) {
        // 检查是否有权限执行集群级别的操作
        if (!authorizedForClusterResource) {
            throw new ClusterAuthorizationException(Errors.CLUSTER_AUTHORIZATION_FAILED.message());
        }
        // 验证资源名称是否为当前节点ID
        validateResourceNameIsNodeId(resource.resourceName());
        // 验证日志级别配置的合法性
        validateLogLevelConfigs(resource.configs());
        // 如果不是仅验证模式，则执行实际的配置修改
        if (!validateOnly) {
            alterLogLevelConfigs(resource.configs());
        }
    }

    /**
     * 执行日志级别配置的修改
     * @param ops 要执行的配置修改操作集合
     */
    void alterLogLevelConfigs(Collection<AlterableConfig> ops) {
        ops.forEach(op -> {
            String loggerName = op.name();
            String logLevel = op.value();
            switch (OpType.forId(op.configOperation())) {
                case SET:
                    // 设置日志级别
                    if (Log4jController.logLevel(loggerName, logLevel)) {
                        log.warn("Updated the log level of {} to {}", loggerName, logLevel);
                    } else {
                        log.error("Failed to update the log level of {} to {}", loggerName, logLevel);
                    }
                    break;
                case DELETE:
                    // 删除（重置）日志级别
                    if (Log4jController.unsetLogLevel(loggerName)) {
                        log.warn("Unset the log level of {}", loggerName);
                    } else {
                        log.error("Failed to unset the log level of {}", loggerName);
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Invalid log4j configOperation: " + op.configOperation());
            }
        });
    }

    /**
     * 验证资源名称是否为当前节点ID
     * @param resourceName 资源名称
     * @throws InvalidRequestException 当资源名称不是整数或与当前节点ID不匹配时抛出
     */
    void validateResourceNameIsNodeId(String resourceName) {
        int requestId;
        try {
            requestId = Integer.parseInt(resourceName);
        } catch (NumberFormatException e) {
            throw new InvalidRequestException("Node id must be an integer, but it is: " +
                resourceName);
        }
        if (requestId != nodeId) {
            throw new InvalidRequestException("Unexpected node id. Expected " + nodeId +
                ", but received " + nodeId);
        }
    }

    /**
     * 验证Logger是否存在
     * @param loggerName Logger名称
     * @throws InvalidConfigurationException 当Logger不存在时抛出
     */
    void validateLoggerNameExists(String loggerName) {
        if (!Log4jController.loggerExists(loggerName)) {
            throw new InvalidConfigurationException("Logger " + loggerName + " does not exist!");
        }
    }

    /**
     * 验证日志级别配置的合法性
     * @param ops 要验证的配置操作集合
     * @throws InvalidConfigurationException 当配置不合法时抛出
     * @throws InvalidRequestException 当操作类型不支持时抛出
     */
    void validateLogLevelConfigs(Collection<AlterableConfig> ops) {
        ops.forEach(op -> {
            String loggerName = op.name();
            switch (OpType.forId(op.configOperation())) {
                case SET:
                    // 验证Logger存在且日志级别有效
                    validateLoggerNameExists(loggerName);
                    String logLevel = op.value();
                    if (!LogLevelConfig.VALID_LOG_LEVELS.contains(logLevel)) {
                        throw new InvalidConfigurationException("Cannot set the log level of " +
                            loggerName + " to " + logLevel + " as it is not a supported log level. " +
                            "Valid log levels are " + VALID_LOG_LEVELS_STRING);
                    }
                    break;
                case DELETE:
                    // 验证Logger存在且不是ROOT logger
                    validateLoggerNameExists(loggerName);
                    if (loggerName.equals(Log4jController.ROOT_LOGGER())) {
                        throw new InvalidRequestException("Removing the log level of the " +
                            Log4jController.ROOT_LOGGER() + " logger is not allowed");
                    }
                    break;
                case APPEND:
                    throw new InvalidRequestException(OpType.APPEND +
                        " operation is not allowed for the " + BROKER_LOGGER + " resource");
                case SUBTRACT:
                    throw new InvalidRequestException(OpType.SUBTRACT +
                        " operation is not allowed for the " + BROKER_LOGGER + " resource");
                default:
                    throw new InvalidRequestException("Unknown operation type " +
                        (int) op.configOperation() + " is not allowed for the " +
                        BROKER_LOGGER + " resource");
            }
        });
    }
}
