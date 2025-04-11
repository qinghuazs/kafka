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

package org.apache.kafka.common.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 此类包含了Kafka应用程序日志相关的日志级别配置定义。
 * 这些配置基于KIP-412提案实现，用于统一管理Kafka的日志级别。
 * KIP Kafka Improvement Proposal Kafka改进提案
 * 
 * 日志级别从高到低依次为：FATAL > ERROR > WARN > INFO > DEBUG > TRACE
 * 配置较高级别后，会自动输出该级别及更高级别的日志。
 */
public class LogLevelConfig {
    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /**
     * <code>FATAL</code>级别用于指示非常严重的错误，这类错误会导致Kafka broker终止运行。
     * 
     * 使用场景：
     * - 系统级致命错误
     * - 无法恢复的运行时异常
     * - 需要立即人工干预的严重问题
     */
    public static final String FATAL_LOG_LEVEL = "FATAL";

    /**
     * <code>ERROR</code>级别用于指示错误事件，这类错误虽然严重但不会导致broker终止运行。
     * 
     * 使用场景：
     * - 业务流程中的重要错误
     * - 需要立即关注但系统仍可继续运行的问题
     * - 外部依赖服务的重要异常
     */
    public static final String ERROR_LOG_LEVEL = "ERROR";

    /**
     * <code>WARN</code>级别用于指示潜在的有害情况，这类情况需要注意但不一定是错误。
     * 
     * 使用场景：
     * - 性能下降警告
     * - 即将达到系统限制
     * - 配置不当的提示
     * - 可能导致问题的潜在风险
     */
    public static final String WARN_LOG_LEVEL = "WARN";

    /**
     * <code>INFO</code>级别用于记录粗粒度的正常Kafka运行事件信息。
     * 这是生产环境中最常用的日志级别。
     * 
     * 使用场景：
     * - 系统启动和关闭
     * - 配置加载信息
     * - 连接建立和断开
     * - 重要的状态变更
     */
    public static final String INFO_LOG_LEVEL = "INFO";

    /**
     * <code>DEBUG</code>级别用于记录细粒度的信息事件，主要用于调试Kafka。
     * 
     * 使用场景：
     * - 详细的处理流程信息
     * - 重要方法的输入输出
     * - 系统运行时的详细状态
     * - 问题诊断和性能调优
     */
    public static final String DEBUG_LOG_LEVEL = "DEBUG";

    /**
     * <code>TRACE</code>级别用于记录最详细的信息事件，比DEBUG级别更加详细。
     * 这个级别在生产环境中很少使用，主要用于开发调试。
     * 
     * 使用场景：
     * - 方法进入和退出的跟踪
     * - 循环中的变量变化
     * - 详细的数据处理过程
     * - 底层组件的交互细节
     */
    public static final String TRACE_LOG_LEVEL = "TRACE";

    /**
     * 定义了所有有效的日志级别集合。
     * 这个集合用于在配置和运行时验证日志级别的有效性。
     * 包含了从最高级别(FATAL)到最低级别(TRACE)的所有合法日志级别。
     */
    public static final Set<String> VALID_LOG_LEVELS = new HashSet<>(Arrays.asList(
            FATAL_LOG_LEVEL, ERROR_LOG_LEVEL, WARN_LOG_LEVEL,
            INFO_LOG_LEVEL, DEBUG_LOG_LEVEL, TRACE_LOG_LEVEL
    ));
}
