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

package org.apache.kafka.trogdor.coordinator;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse;
import org.apache.kafka.trogdor.rest.CreateTaskRequest;
import org.apache.kafka.trogdor.rest.DestroyTaskRequest;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.StopTaskRequest;
import org.apache.kafka.trogdor.rest.TaskRequest;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;
import org.apache.kafka.trogdor.rest.UptimeResponse;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.concurrent.ThreadLocalRandom;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * Trogdor协调器。
 *
 * 该协调器负责管理集群中的代理进程。它作为Trogdor故障注入系统的核心组件，
 * 协调和管理各个代理节点的任务执行。
 */
public final class Coordinator {

    /**
     * 协调器的默认端口号
     */
    public static final int DEFAULT_PORT = 8889;

    /**
     * 协调器的启动时间（以毫秒为单位）
     * 用于跟踪协调器的运行时间和状态监控
     */
    private final long startTimeMs;

    /**
     * 任务管理器实例
     * 负责创建、停止、销毁和管理所有任务的生命周期
     */
    private final TaskManager taskManager;

    /**
     * REST服务器实例
     * 提供HTTP API接口，用于接收和处理外部请求
     */
    private final JsonRestServer restServer;

    /**
     * 时间服务实例
     * 用于获取系统时间，支持测试时的时间模拟
     */
    private final Time time;

    /**
     * 创建新的协调器实例
     *
     * @param platform      平台对象，提供运行时环境和配置信息
     * @param scheduler     调度器，用于管理任务的定时执行
     * @param restServer    REST服务器，处理HTTP请求
     * @param resource      REST资源对象，提供API端点
     * @param firstWorkerId 第一个工作节点的ID
     */
    public Coordinator(Platform platform, Scheduler scheduler, JsonRestServer restServer,
                       CoordinatorRestResource resource, long firstWorkerId) {
        // 初始化时间服务
        this.time = scheduler.time();
        // 记录启动时间
        this.startTimeMs = time.milliseconds();
        // 创建任务管理器实例
        this.taskManager = new TaskManager(platform, scheduler, firstWorkerId);
        // 设置REST服务器
        this.restServer = restServer;
        // 将当前协调器实例与REST资源关联
        resource.setCoordinator(this);
    }

    /**
     * 获取协调器的监听端口
     * @return 返回REST服务器的端口号
     */
    public int port() {
        return this.restServer.port();
    }

    /**
     * 获取协调器的状态信息
     * @return 包含启动时间的状态响应对象
     */
    public CoordinatorStatusResponse status() throws Exception {
        return new CoordinatorStatusResponse(startTimeMs);
    }

    /**
     * 获取协调器的运行时间信息
     * @return 包含启动时间和当前时间的运行时间响应对象
     */
    public UptimeResponse uptime() {
        return new UptimeResponse(startTimeMs, time.milliseconds());
    }

    /**
     * 创建新的任务
     * @param request 创建任务的请求对象，包含任务ID和规格
     */
    public void createTask(CreateTaskRequest request) throws Throwable {
        taskManager.createTask(request.id(), request.spec());
    }

    /**
     * 停止指定的任务
     * @param request 停止任务的请求对象，包含任务ID
     */
    public void stopTask(StopTaskRequest request) throws Throwable {
        taskManager.stopTask(request.id());
    }

    /**
     * 销毁指定的任务
     * @param request 销毁任务的请求对象，包含任务ID
     */
    public void destroyTask(DestroyTaskRequest request) throws Throwable {
        taskManager.destroyTask(request.id());
    }

    /**
     * 获取符合条件的任务列表
     * @param request 任务查询请求对象，包含查询条件
     * @return 任务列表响应对象
     */
    public TasksResponse tasks(TasksRequest request) throws Exception {
        return taskManager.tasks(request);
    }

    /**
     * 获取指定任务的状态
     * @param request 任务查询请求对象，包含任务ID
     * @return 任务状态对象
     */
    public TaskState task(TaskRequest request) throws Exception {
        return taskManager.task(request);
    }

    /**
     * 开始关闭协调器
     * @param stopAgents 是否同时停止所有代理
     */
    public void beginShutdown(boolean stopAgents) throws Exception {
        // 首先关闭REST服务器，停止接收新请求
        restServer.beginShutdown();
        // 然后关闭任务管理器，停止所有任务
        taskManager.beginShutdown(stopAgents);
    }

    /**
     * 等待协调器完全关闭
     * 确保REST服务器和任务管理器都已完全停止
     */
    public void waitForShutdown() throws Exception {
        // 等待REST服务器完全关闭
        restServer.waitForShutdown();
        // 等待任务管理器完全关闭
        taskManager.waitForShutdown();
    }

    /**
     * 协调器的主入口方法
     * 负责解析命令行参数、初始化和启动协调器进程
     *
     * @param args 命令行参数数组
     * @throws Exception 如果在启动过程中发生错误
     */
    public static void main(String[] args) throws Exception {
        // 创建命令行参数解析器
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("trogdor-coordinator")
            .defaultHelp(true)
            .description("The Trogdor fault injection coordinator");

        // 添加配置文件参数
        parser.addArgument("--coordinator.config", "-c")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("config")
            .metavar("CONFIG")
            .help("配置文件路径");

        // 添加节点名称参数
        parser.addArgument("--node-name", "-n")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("node_name")
            .metavar("NODE_NAME")
            .help("当前节点的名称");

        // 解析命令行参数
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            // 如果没有提供参数，打印帮助信息并退出
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                // 如果参数解析出错，处理错误并退出
                parser.handleError(e);
                Exit.exit(1);
            }
        }

        // 获取解析后的参数值
        String configPath = res.getString("config");
        String nodeName = res.getString("node_name");

        // 解析配置文件，创建平台实例
        Platform platform = Platform.Config.parse(nodeName, configPath);
        
        // 创建REST服务器，使用配置的协调器端口
        JsonRestServer restServer = new JsonRestServer(
            Node.Util.getTrogdorCoordinatorPort(platform.curNode()));
        
        // 创建REST资源对象
        CoordinatorRestResource resource = new CoordinatorRestResource();
        
        System.out.println("Starting coordinator process.");
        
        // 创建协调器实例
        // 使用系统调度器和随机生成的首个工作节点ID
        final Coordinator coordinator = new Coordinator(platform, Scheduler.SYSTEM,
            restServer, resource, ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE / 2));
        
        // 启动REST服务器
        restServer.start(resource);
        
        // 添加关闭钩子，确保程序退出时能够正常关闭
        Exit.addShutdownHook("coordinator-shutdown-hook", () -> {
            System.out.println("Running coordinator shutdown hook.");
            try {
                // 开始关闭协调器，不停止代理
                coordinator.beginShutdown(false);
                // 等待协调器完全关闭
                coordinator.waitForShutdown();
            } catch (Exception e) {
                System.out.println("Got exception while running coordinator shutdown hook. " + e);
            }
        });
        
        // 等待协调器关闭
        coordinator.waitForShutdown();
    }
}
