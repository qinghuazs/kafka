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

package kafka.log

import java.lang.{Long => JLong}
import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import kafka.server.{KafkaConfig, KafkaRaftServer}
import kafka.server.metadata.BrokerMetadataPublisher.info
import kafka.utils.threadsafe
import kafka.utils.{CoreUtils, Logging, Pool}
import org.apache.kafka.common.{DirectoryId, KafkaException, TopicPartition, Uuid}
import org.apache.kafka.common.utils.{Exit, KafkaThread, Time, Utils}
import org.apache.kafka.common.errors.{InconsistentTopicIdException, KafkaStorageException, LogDirNotFoundException}

import scala.jdk.CollectionConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}
import org.apache.kafka.image.TopicsImage
import org.apache.kafka.metadata.ConfigRepository
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, PropertiesUtils}

import java.util.{Collections, OptionalLong, Properties}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.{FileLock, Scheduler}
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig, LogDirFailureChannel, ProducerStateManagerConfig, RemoteIndexCache}
import org.apache.kafka.storage.internals.checkpoint.{CleanShutdownFileHandler, OffsetCheckpointFile}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.util

/**
 * Kafka日志管理子系统的入口点。日志管理器负责日志的创建、检索和清理。
 * 所有的读写操作都委托给各个日志实例处理。
 *
 * 日志管理器在一个或多个目录中维护日志。新的日志会被创建在包含最少日志数量的数据目录中。
 * 系统不会在创建后移动分区或基于大小和I/O速率进行平衡。
 *
 * 一个后台线程通过定期截断多余的日志段来处理日志保留。
 */
@threadsafe
class LogManager(
    // 日志目录列表
    logDirs: Seq[File],
    // 初始离线目录列表
    initialOfflineDirs: Seq[File],
    // 配置仓库
    configRepository: ConfigRepository,
    // 初始默认日志配置
    val initialDefaultConfig: LogConfig,
    // 清理器配置
    val cleanerConfig: CleanerConfig,
    // 每个数据目录的恢复线程数
    recoveryThreadsPerDataDir: Int,
    // 刷新检查的时间间隔(毫秒)
    val flushCheckMs: Long,
    // 恢复点检查点刷新间隔(毫秒)
    val flushRecoveryOffsetCheckpointMs: Long,
    // 起始偏移量检查点刷新间隔(毫秒)
    val flushStartOffsetCheckpointMs: Long,
    // 日志保留检查间隔(毫秒)
    val retentionCheckMs: Long,
    // 最大事务超时时间(毫秒)
    val maxTransactionTimeoutMs: Int,
    // 生产者状态管理器配置
    val producerStateManagerConfig: ProducerStateManagerConfig,
    // 生产者ID过期检查间隔(毫秒)
    val producerIdExpirationCheckIntervalMs: Int,
    // Broker间协议版本
    interBrokerProtocolVersion: MetadataVersion,
    // 调度器
    scheduler: Scheduler,
    // Broker主题统计信息
    brokerTopicStats: BrokerTopicStats,
    // 日志目录故障通道
    logDirFailureChannel: LogDirFailureChannel,
    // 时间服务
    time: Time,
    // 是否启用远程存储系统
    remoteStorageSystemEnable: Boolean,
    // 初始任务延迟时间(毫秒)
    val initialTaskDelayMs: Long) extends Logging {

  import LogManager._

  // 度量指标组
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  // 日志创建或删除的同步锁
  private val logCreationOrDeletionLock = new Object
  // 当前活跃的日志映射表，键为主题分区，值为统一日志对象
  private val currentLogs = new Pool[TopicPartition, UnifiedLog]()
  // 未来日志映射表。未来日志存储在带有"-future"后缀的目录中。
  // 当用户想要在同一个broker上将副本从一个日志目录移动到另一个日志目录时创建。
  // 当未来日志追赶上当前日志后，其目录将被重命名以替换当前分区的日志
  private val futureLogs = new Pool[TopicPartition, UnifiedLog]()
  // 待删除日志队列，每个元素包含要删除的日志对象和计划删除的时间
  private val logsToBeDeleted = new LinkedBlockingQueue[(UnifiedLog, Long)]()

  // 游离分区到游离日志的映射。保存在broker上检测到的所有游离日志。
  // 用于测试可见
  private val strayLogs = new Pool[TopicPartition, UnifiedLog]()

  // 活跃的日志目录队列
  private val _liveLogDirs: ConcurrentLinkedQueue[File] = createAndValidateLogDirs(logDirs, initialOfflineDirs)
  // 当前默认配置，使用@volatile保证可见性
  @volatile private var _currentDefaultConfig = initialDefaultConfig
  // 每个数据目录的恢复线程数，使用@volatile保证可见性
  @volatile private var numRecoveryThreadsPerDataDir = recoveryThreadsPerDataDir

  // 该映射包含所有正在加载和初始化日志的分区。如果这些分区的日志配置
  // 同时被更新，对应的映射条目会被设置为"true"，这将触发配置在初始化
  // 完成后重新加载(以获取最新的配置值)。
  // 参见KAFKA-8813了解更多关于竞态条件的细节
  // 用于测试可见
  private[log] val partitionsInitializing = new ConcurrentHashMap[TopicPartition, Boolean]().asScala

  /**
   * 重新配置默认的日志配置
   * 
   * @param logConfig 新的日志配置
   */
  def reconfigureDefaultLogConfig(logConfig: LogConfig): Unit = {
    // 更新当前默认配置
    this._currentDefaultConfig = logConfig
  }

  /**
   * 获取当前默认的日志配置
   * 
   * @return 当前默认的日志配置
   */
  def currentDefaultConfig: LogConfig = _currentDefaultConfig

  /**
   * 获取当前活跃的日志目录列表
   * 如果所有目录都是活跃的，返回原始目录列表
   * 否则返回活跃目录的子集
   * 
   * @return 活跃的日志目录列表
   */
  def liveLogDirs: Seq[File] = {
    if (_liveLogDirs.size == logDirs.size)
      logDirs
    else
      _liveLogDirs.asScala.toBuffer
  }

  // 用于测试可见
  // 为活跃的日志目录创建锁文件
  private[log] val dirLocks = lockLogDirs(liveLogDirs)
  // 加载目录ID映射，键为目录路径，值为目录UUID
  private val directoryIds: mutable.Map[String, Uuid] = loadDirectoryIds(liveLogDirs)
  // 获取所有目录ID的集合
  def directoryIdsSet: Predef.Set[Uuid] = directoryIds.values.toSet

  // 恢复点检查点文件映射，使用@volatile保证可见性
  // 每个活跃目录对应一个检查点文件，用于记录日志恢复点
  @volatile private var recoveryPointCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel))).toMap
  // 日志起始偏移量检查点文件映射，使用@volatile保证可见性
  // 每个活跃目录对应一个检查点文件，用于记录日志起始偏移量
  @volatile private var logStartOffsetCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel))).toMap

  // 首选日志目录映射，记录主题分区与其首选日志目录的对应关系
  private val preferredLogDirs = new ConcurrentHashMap[TopicPartition, String]()

  /**
   * 检查是否存在离线的日志目录
   * 
   * @return 如果存在离线目录返回true，否则返回false
   */
  def hasOfflineLogDirs(): Boolean = offlineLogDirs.nonEmpty

  /**
   * 检查给定UUID的目录是否在线
   * 
   * @param uuid 目录UUID
   * @return 如果目录在线返回true，否则返回false
   */
  def onlineLogDirId(uuid: Uuid): Boolean = directoryIds.exists(_._2 == uuid)

  /**
   * 获取所有离线的日志目录
   * 通过从所有日志目录中移除活跃目录来计算
   * 
   * @return 离线日志目录的集合
   */
  private def offlineLogDirs: Iterable[File] = {
    // 创建包含所有日志目录的集合
    val logDirsSet = mutable.Set[File]() ++= logDirs
    // 移除所有活跃的目录
    _liveLogDirs.forEach(dir => logDirsSet -= dir)
    // 返回剩余的离线目录
    logDirsSet
  }

  // 存储每个日志目录的干净关闭标志的映射
  // 用于跟踪每个日志目录在上次关闭时是否正常关闭
  private val hadCleanShutdownFlags = new ConcurrentHashMap[String, Boolean]()

  // 存储每个日志目录在启动时是否已完成加载所有日志的标志映射
  // 用于跟踪启动过程中日志目录的加载状态
  private val loadLogsCompletedFlags = new ConcurrentHashMap[String, Boolean]()

  // 日志清理器实例，使用@volatile确保多线程可见性
  @volatile private var _cleaner: LogCleaner = _
  // 提供对日志清理器的访问
  private[kafka] def cleaner: LogCleaner = _cleaner

  // 注册离线日志目录数量的度量指标
  metricsGroup.newGauge("OfflineLogDirectoryCount", () => offlineLogDirs.size)

  // 为每个日志目录注册离线状态的度量指标
  // 0表示目录在线，1表示目录离线
  for (dir <- logDirs) {
    metricsGroup.newGauge("LogDirectoryOffline",
      () => if (_liveLogDirs.contains(dir)) 0 else 1,
      Map("logDirectory" -> dir.getAbsolutePath).asJava)
  }

  /**
   * 创建并验证给定的日志目录（不包括已知的离线目录），具体执行以下操作：
   * <ol>
   * <li> 确保目录列表中没有重复项
   * <li> 如果目录不存在则创建
   * <li> 检查每个路径是否为可读目录
   * </ol>
   *
   * @param dirs 需要验证的日志目录列表
   * @param initialOfflineDirs 初始离线目录列表
   * @return 包含所有有效且可用的日志目录的并发队列
   */
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    // 创建一个并发队列来存储活跃的日志目录
    val liveLogDirs = new ConcurrentLinkedQueue[File]()
    // 用于检测重复目录的规范路径集合
    val canonicalPaths = mutable.HashSet.empty[String]

    for (dir <- dirs) {
      try {
        // 检查目录是否在初始离线目录列表中
        if (initialOfflineDirs.contains(dir))
          throw new IOException(s"Failed to load ${dir.getAbsolutePath} during broker startup")

        // 如果目录不存在，创建它
        if (!dir.exists) {
          info(s"Log directory ${dir.getAbsolutePath} not found, creating it.")
          val created = dir.mkdirs()
          if (!created)
            throw new IOException(s"Failed to create data directory ${dir.getAbsolutePath}")
          // 确保父目录的更改被持久化到磁盘
          Utils.flushDir(dir.toPath.toAbsolutePath.normalize.getParent)
        }
        // 验证目录是否可读
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(s"${dir.getAbsolutePath} is not a readable log directory.")

        // 获取规范路径并检查重复
        // getCanonicalPath()在文件系统查询失败或路径无效（如包含Nul字符）时会抛出IOException
        // 由于难以区分这两种情况，我们统一将目录标记为离线
        if (!canonicalPaths.add(dir.getCanonicalPath))
          throw new KafkaException(s"Duplicate log directory found: ${dirs.mkString(", ")}")

        // 将验证通过的目录添加到活跃目录列表
        liveLogDirs.add(dir)
      } catch {
        case e: IOException =>
          // 如果目录验证失败，将其添加到离线目录通道
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Failed to create or validate data directory ${dir.getAbsolutePath}", e)
      }
    }
    // 如果没有可用的日志目录，终止broker启动
    if (liveLogDirs.isEmpty) {
      fatal(s"Shutdown broker because none of the specified log dirs from ${dirs.mkString(", ")} can be created or validated")
      Exit.halt(1)
    }

    liveLogDirs
  }

  /**
   * 调整每个数据目录的恢复线程池大小
   * 用于动态调整日志恢复过程中的并发度
   *
   * @param newSize 新的线程池大小
   */
  def resizeRecoveryThreadPool(newSize: Int): Unit = {
    // 记录线程池大小的调整信息
    info(s"Resizing recovery thread pool size for each data dir from $numRecoveryThreadsPerDataDir to $newSize")
    // 更新每个数据目录的恢复线程数
    numRecoveryThreadsPerDataDir = newSize
  }

  /**
   * 日志目录故障处理器。负责停止指定目录的日志清理并处理相关资源。
   * 当检测到日志目录发生故障时（如磁盘错误、权限问题等），此方法会：
   * 1. 将目录从活跃目录列表中移除
   * 2. 清理相关的检查点信息
   * 3. 停止该目录的日志清理
   * 4. 关闭该目录下的所有日志
   * 5. 释放目录锁
   *
   * @param dir 发生故障的日志目录的绝对路径
   */
  def handleLogDirFailure(dir: String): Unit = {
    // 记录停止服务的警告信息
    warn(s"Stopping serving logs in dir $dir")
    // 使用同步锁确保目录操作的原子性
    logCreationOrDeletionLock synchronized {
      // 从活跃目录列表中移除故障目录
      _liveLogDirs.remove(new File(dir))
      // 移除目录ID映射
      directoryIds.remove(dir)
      // 如果没有可用的日志目录，终止broker
      if (_liveLogDirs.isEmpty) {
        fatal(s"Shutdown broker because all log dirs in ${logDirs.mkString(", ")} have failed")
        Exit.halt(1)
      }

      // 从检查点映射中移除故障目录的记录
      recoveryPointCheckpoints = recoveryPointCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      logStartOffsetCheckpoints = logStartOffsetCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      // 如果日志清理器存在，处理目录故障
      if (cleaner != null)
        cleaner.handleLogDirFailure(dir)

      /**
       * 内部函数：移除指定日志池中属于故障目录的日志
       * @param logs 日志池（当前日志或未来日志）
       * @return 被移除的主题分区集合
       */
      def removeOfflineLogs(logs: Pool[TopicPartition, UnifiedLog]): Iterable[TopicPartition] = {
        // 收集故障目录下的所有主题分区
        val offlineTopicPartitions: Iterable[TopicPartition] = logs.collect {
          case (tp, log) if log.parentDir == dir => tp
        }
        // 移除每个离线分区的日志并关闭相关资源
        offlineTopicPartitions.foreach { topicPartition => {
          val removedLog = removeLogAndMetrics(logs, topicPartition)
          removedLog.foreach {
            log => log.closeHandlers()
          }
        }}

        offlineTopicPartitions
      }

      // 分别处理当前日志和未来日志
      val offlineCurrentTopicPartitions = removeOfflineLogs(currentLogs)
      val offlineFutureTopicPartitions = removeOfflineLogs(futureLogs)

      // 记录离线分区信息
      warn(s"Logs for partitions ${offlineCurrentTopicPartitions.mkString(",")} are offline and " +
           s"logs for future partitions ${offlineFutureTopicPartitions.mkString(",")} are offline due to failure on log directory $dir")
      // 释放目录锁
      dirLocks.filter(_.file.getParent == dir).foreach(dir => CoreUtils.swallow(dir.destroy(), this))
    }
  }

  /**
   * 锁定所有给定的日志目录
   * 通过在每个目录中创建.lock文件来实现目录级别的互斥访问
   * 这个机制确保同一时间只有一个Kafka实例可以访问特定的日志目录
   *
   * @param dirs 需要锁定的目录列表
   * @return 成功获取的文件锁列表
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.flatMap { dir =>
      try {
        // 在目录中创建.lock文件
        val lock = new FileLock(new File(dir, LockFileName))
        // 尝试获取锁，如果失败说明目录已被其他进程使用
        if (!lock.tryLock())
          throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParent +
            ". A Kafka instance in another process or thread is using this directory.")
        Some(lock)
      } catch {
        case e: IOException =>
          // 如果发生IO异常，将目录标记为离线
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while locking directory $dir", e)
          None
      }
    }
  }

  /**
   * 根据目录的绝对路径获取其对应的UUID
   * 
   * @param dir 目录的绝对路径
   * @return 如果目录存在对应的UUID则返回Some(UUID)，否则返回None
   */
  def directoryId(dir: String): Option[Uuid] = directoryIds.get(dir)

  /**
   * 根据UUID获取对应的目录路径
   * 
   * @param uuid 目录的UUID
   * @return 如果UUID存在对应的目录则返回Some(路径)，否则返回None
   */
  def directoryPath(uuid: Uuid): Option[String] = directoryIds.find(_._2 == uuid).map(_._1)

  /**
   * 为每个包含meta.properties文件的目录加载或生成目录ID
   * 如果meta.properties文件中不包含目录ID，则会生成一个新的ID并持久化回meta.properties文件
   * 没有meta.properties文件的目录不会被分配目录ID
   * 
   * @param logDirs 需要加载目录ID的日志目录列表
   * @return 目录路径到目录ID的映射表
   */
  private def loadDirectoryIds(logDirs: Seq[File]): mutable.Map[String, Uuid] = {
    // 创建一个可变的HashMap来存储目录路径到目录ID的映射
    val result = mutable.HashMap[String, Uuid]()
    // 遍历每个日志目录
    logDirs.foreach(logDir => {
      try {
        // 读取meta.properties文件
        val props = PropertiesUtils.readPropertiesFile(
          new File(logDir, MetaPropertiesEnsemble.META_PROPERTIES_NAME).getAbsolutePath)
        // 构建元数据属性对象
        val metaProps = new MetaProperties.Builder(props).build()
        // 如果存在目录ID，则添加到映射表中
        metaProps.directoryId().ifPresent(directoryId => {
          result += (logDir.getAbsolutePath -> directoryId)
        })
      } catch {
        case _: NoSuchFileException =>
          // 如果meta.properties文件不存在，记录信息日志
          info(s"No meta.properties file found in $logDir.")
         case e: IOException =>
          // 如果发生IO异常，将目录标记为离线
          logDirFailureChannel.maybeAddOfflineLogDir(logDir.getAbsolutePath, s"Disk error while loading ID $logDir", e)
       }
    })
    result
  }

  /**
   * 将日志添加到待删除队列中
   * 
   * @param log 需要删除的统一日志对象
   */
  private def addLogToBeDeleted(log: UnifiedLog): Unit = {
    this.logsToBeDeleted.add((log, time.milliseconds()))
  }

  /**
   * 添加游离日志到游离日志池中
   * 游离日志是指那些不再属于当前活跃主题分区的日志
   * 
   * @param strayPartition 游离日志对应的主题分区
   * @param strayLog 游离日志对象
   */
  def addStrayLog(strayPartition: TopicPartition, strayLog: UnifiedLog): Unit = {
    this.strayLogs.put(strayPartition, strayLog)
  }

  /**
   * 检查是否存在待删除的日志
   * 仅用于测试目的
   * 
   * @return 如果存在待删除的日志返回true，否则返回false
   */
  private[log] def hasLogsToBeDeleted: Boolean = !logsToBeDeleted.isEmpty

  /**
   * 加载日志目录中的日志，处理各种特殊情况（如待删除日志、游离日志等）
   * 
   * @param logDir 日志目录
   * @param hadCleanShutdown 上次是否正常关闭
   * @param recoveryPoints 恢复点映射表
   * @param logStartOffsets 日志起始偏移量映射表
   * @param defaultConfig 默认日志配置
   * @param topicConfigOverrides 主题配置覆盖映射表
   * @param numRemainingSegments 剩余日志段数量映射表
   * @param isStray 判断日志是否为游离日志的函数
   * @return 加载的统一日志对象
   */
  private[log] def loadLog(logDir: File,
                           hadCleanShutdown: Boolean,
                           recoveryPoints: util.Map[TopicPartition, JLong],
                           logStartOffsets: util.Map[TopicPartition, JLong],
                           defaultConfig: LogConfig,
                           topicConfigOverrides: Map[String, LogConfig],
                           numRemainingSegments: ConcurrentMap[String, Integer],
                           isStray: UnifiedLog => Boolean): UnifiedLog = {
    // 从日志目录名解析主题分区信息
    val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
    // 获取日志配置，优先使用主题特定配置，否则使用默认配置
    val config = topicConfigOverrides.getOrElse(topicPartition.topic, defaultConfig)
    // 获取日志恢复点，如果不存在则使用0
    val logRecoveryPoint = recoveryPoints.getOrDefault(topicPartition, 0L)
    // 获取日志起始偏移量，如果不存在则使用0
    val logStartOffset = logStartOffsets.getOrDefault(topicPartition, 0L)

    // 创建统一日志对象
    val log = UnifiedLog(
      dir = logDir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = logRecoveryPoint,
      maxTransactionTimeoutMs = maxTransactionTimeoutMs,
      producerStateManagerConfig = producerStateManagerConfig,
      producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
      scheduler = scheduler,
      time = time,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      lastShutdownClean = hadCleanShutdown,
      topicId = None,
      numRemainingSegments = numRemainingSegments,
      remoteStorageSystemEnable = remoteStorageSystemEnable)

    // 根据日志目录名后缀和状态进行不同处理
    if (logDir.getName.endsWith(UnifiedLog.DeleteDirSuffix)) {
      // 如果是待删除目录，添加到待删除队列
      addLogToBeDeleted(log)
    } else if (logDir.getName.endsWith(UnifiedLog.StrayDirSuffix)) {
      // 如果是游离日志目录，添加到游离日志池
      addStrayLog(topicPartition, log)
      warn(s"Loaded stray log: $logDir")
    } else if (isStray(log)) {
      // 如果日志被判定为游离日志
      // 由于无法阻止主题在所有副本删除前被重新创建
      // 具有离线目录的Broker可能无法检测到它仍持有待删除的副本
      // 并可能在剩余的在线目录中为主题的新实例创建冲突的主题分区
      // 因此，当离线目录重新上线时，我们需要清理旧的副本目录
      log.renameDir(UnifiedLog.logStrayDirName(log.topicPartition), shouldReinitialize = false)
      addStrayLog(log.topicPartition, log)
      warn(s"Log in ${logDir.getAbsolutePath} marked stray and renamed to ${log.dir.getAbsolutePath}")
    } else {
      // 正常日志的处理
      val previous = {
        if (log.isFuture)
          // 如果是未来日志，添加到未来日志池
          this.futureLogs.put(topicPartition, log)
        else
          // 否则添加到当前日志池
          this.currentLogs.put(topicPartition, log)
      }
      // 检查是否存在重复的日志目录
      if (previous != null) {
        if (log.isFuture)
          throw new IllegalStateException(s"Duplicate log directories found: ${log.dir.getAbsolutePath}, ${previous.dir.getAbsolutePath}")
        else
          throw new IllegalStateException(s"Duplicate log directories for $topicPartition are found in both ${log.dir.getAbsolutePath} " +
            s"and ${previous.dir.getAbsolutePath}. It is likely because log directory failure happened while broker was " +
            s"replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two directories " +
            s"for this partition. It is recommended to delete the partition in the log directory that is known to have failed recently.")
      }
    }

    log
  }

  /**
   * 用于创建日志恢复线程的工厂类
   * 主要用于在度量指标中命名日志恢复线程
   * 
   * @param dirPath 日志目录路径
   */
  private class LogRecoveryThreadFactory(val dirPath: String) extends ThreadFactory {
    // 线程计数器
    val threadNum = new AtomicInteger(0)

    override def newThread(runnable: Runnable): Thread = {
      // 创建非守护线程，使用特定的命名格式
      KafkaThread.nonDaemon(logRecoveryThreadName(dirPath, threadNum.getAndIncrement()), runnable)
    }
  }

  /**
   * 为每个日志目录创建唯一的日志恢复线程名
   * 格式为：prefix-dirPath-threadNum，例如："log-recovery-/tmp/kafkaLogs-0"
   * 
   * @param dirPath 日志目录路径
   * @param threadNum 线程编号
   * @param prefix 线程名前缀，默认为"log-recovery"
   * @return 格式化的线程名
   */
  private def logRecoveryThreadName(dirPath: String, threadNum: Int, prefix: String = "log-recovery"): String = s"$prefix-$dirPath-$threadNum"

  /**
   * 减少剩余日志数量
   * 
   * @param numRemainingLogs 剩余日志数量的并发映射表
   * @param path 日志路径
   * @return 减少1后的剩余日志数量
   */
  private[log] def decNumRemainingLogs(numRemainingLogs: ConcurrentMap[String, Int], path: String): Int = {
    // 确保路径不为空
    require(path != null, "path cannot be null to update remaining logs metric.")
    // 原子性地将值减1
    numRemainingLogs.compute(path, (_, oldVal) => oldVal - 1)
  }

  /**
   * 恢复并加载所有给定数据目录中的日志
   * 该方法是Kafka日志恢复的核心实现，主要完成以下工作：
   * 1. 并发处理多个日志目录的恢复
   * 2. 检查每个目录的干净关闭状态
   * 3. 读取恢复点和日志起始偏移量检查点
   * 4. 并发加载和恢复日志文件
   * 5. 收集恢复过程的度量指标
   * 6. 处理各种异常情况
   *
   * @param defaultConfig 默认的日志配置
   * @param topicConfigOverrides 主题级别的配置覆盖
   * @param isStray 判断日志是否为游离日志的函数
   */
  private[log] def loadLogs(defaultConfig: LogConfig, topicConfigOverrides: Map[String, LogConfig], isStray: UnifiedLog => Boolean): Unit = {
    // 记录开始加载日志的信息
    info(s"Loading logs from log dirs $liveLogDirs")
    // 记录开始时间
    val startMs = time.hiResClockMs()
    // 创建线程池数组，用于并发处理日志恢复
    val threadPools = ArrayBuffer.empty[ExecutorService]
    // 存储离线目录及其IO异常信息
    val offlineDirs = mutable.Set.empty[(String, IOException)]
    // 存储所有目录的恢复任务
    val jobs = ArrayBuffer.empty[Seq[Future[_]]]
    // 记录总日志数
    var numTotalLogs = 0
    // 存储每个日志目录剩余待恢复的日志数量，用于度量指标
    val numRemainingLogs: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int]
    // 存储每个恢复线程剩余待恢复的日志段数量，用于度量指标
    val numRemainingSegments: ConcurrentMap[String, Integer] = new ConcurrentHashMap[String, Integer]

    /**
     * 处理IO异常的内部函数
     * 将发生异常的目录添加到离线目录集合，并记录错误日志
     */
    def handleIOException(logDirAbsolutePath: String, e: IOException): Unit = {
      offlineDirs.add((logDirAbsolutePath, e))
      error(s"Error while loading log dir $logDirAbsolutePath", e)
    }

    // 存储未正常关闭的日志目录
    val uncleanLogDirs = mutable.Buffer.empty[String]
    // 遍历所有活跃的日志目录进行恢复
    for (dir <- liveLogDirs) {
      val logDirAbsolutePath = dir.getAbsolutePath
      var hadCleanShutdown: Boolean = false
      try {
        // 为每个目录创建固定大小的线程池
        val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
          new LogRecoveryThreadFactory(logDirAbsolutePath))
        threadPools.append(pool)

        // 检查是否存在干净关闭标记文件
        val cleanShutdownFileHandler = new CleanShutdownFileHandler(dir.getPath)
        if (cleanShutdownFileHandler.exists()) {
          // 缓存干净关闭状态并删除标记文件
          // 这样如果在加载日志时broker崩溃，下次启动时会被视为硬关闭 (KAFKA-10471)
          cleanShutdownFileHandler.delete()
          hadCleanShutdown = true
        }
        hadCleanShutdownFlags.put(logDirAbsolutePath, hadCleanShutdown)

        // 读取恢复点检查点文件
        val recoveryPoints: util.Map[TopicPartition, JLong] = try {
          this.recoveryPointCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading recovery-point-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting the recovery checkpoint to 0", e)
            Collections.emptyMap[TopicPartition, JLong]
        }

        // 读取日志起始偏移量检查点文件
        val logStartOffsets: util.Map[TopicPartition, JLong] = try {
          this.logStartOffsetCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading log-start-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting to the base offset of the first segment", e)
            Collections.emptyMap[TopicPartition, JLong]
        }

        // 获取需要加载的日志目录列表
        // 过滤掉远程日志索引缓存目录和元数据主题目录
        val logsToLoad = Option(dir.listFiles).getOrElse(Array.empty).filter(logDir =>
          logDir.isDirectory &&
            !logDir.getName.equals(RemoteIndexCache.DIR_NAME) &&
            UnifiedLog.parseTopicPartitionName(logDir).topic != KafkaRaftServer.MetadataTopic)
        // 更新总日志数和剩余日志数
        numTotalLogs += logsToLoad.length
        numRemainingLogs.put(logDirAbsolutePath, logsToLoad.length)
        loadLogsCompletedFlags.put(logDirAbsolutePath, logsToLoad.isEmpty)

        // 根据日志目录状态记录相应的信息
        if (logsToLoad.isEmpty) {
          info(s"No logs found to be loaded in $logDirAbsolutePath")
        } else if (hadCleanShutdown) {
          info(s"Skipping recovery of ${logsToLoad.length} logs from $logDirAbsolutePath since " +
            "clean shutdown file was found")
        } else {
          info(s"Recovering ${logsToLoad.length} logs from $logDirAbsolutePath since no " +
            "clean shutdown file was found")
          uncleanLogDirs.append(logDirAbsolutePath)
        }

        // 为每个日志创建加载任务
        val jobsForDir = logsToLoad.map { logDir =>
          val runnable: Runnable = () => {
            debug(s"Loading log $logDir")
            var log = None: Option[UnifiedLog]
            val logLoadStartMs = time.hiResClockMs()
            try {
              // 加载单个日志
              log = Some(loadLog(logDir, hadCleanShutdown, recoveryPoints, logStartOffsets,
                defaultConfig, topicConfigOverrides, numRemainingSegments, isStray))
            } catch {
              case e: IOException =>
                handleIOException(logDirAbsolutePath, e)
              case e: KafkaStorageException if e.getCause.isInstanceOf[IOException] =>
                // 处理存储异常，如写入LeaderEpochFileCache时的异常
                // 由于在转换为KafkaStorageException时已经处理过异常，这里可以忽略
            } finally {
              // 计算加载耗时并更新进度信息
              val logLoadDurationMs = time.hiResClockMs() - logLoadStartMs
              val remainingLogs = decNumRemainingLogs(numRemainingLogs, logDirAbsolutePath)
              val currentNumLoaded = logsToLoad.length - remainingLogs
              // 记录加载结果
              log match {
                case Some(loadedLog) => info(s"Completed load of $loadedLog with ${loadedLog.numberOfSegments} segments, " +
                  s"local-log-start-offset ${loadedLog.localLogStartOffset()} and log-end-offset ${loadedLog.logEndOffset} in ${logLoadDurationMs}ms " +
                  s"($currentNumLoaded/${logsToLoad.length} completed in $logDirAbsolutePath)")
                case None => info(s"Error while loading logs in $logDir in ${logLoadDurationMs}ms ($currentNumLoaded/${logsToLoad.length} completed in $logDirAbsolutePath)")
              }

              // 如果目录下所有日志都已加载完成，更新标志
              if (remainingLogs == 0) {
                loadLogsCompletedFlags.put(logDirAbsolutePath, true)
              }
            }
          }
          runnable
        }

        // 提交该目录的所有加载任务
        jobs += jobsForDir.map(pool.submit)
      } catch {
        case e: IOException =>
          handleIOException(logDirAbsolutePath, e)
      }
    }

    try {
      // 添加日志恢复度量指标
      addLogRecoveryMetrics(numRemainingLogs, numRemainingSegments)
      // 等待所有任务完成
      for (dirJobs <- jobs) {
        dirJobs.foreach(_.get)
      }

      // 将所有离线目录添加到故障通道
      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while loading log dir $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        error(s"There was an error in one of the threads during logs loading: ${e.getCause}")
        throw e.getCause
    } finally {
      // 清理度量指标和关闭线程池
      removeLogRecoveryMetrics()
      threadPools.foreach(_.shutdown())
    }

    // 记录总体恢复完成信息
    val elapsedMs = time.hiResClockMs() - startMs
    val printedUncleanLogDirs = if (uncleanLogDirs.isEmpty) "" else s" (unclean log dirs = $uncleanLogDirs)"
    info(s"Loaded $numTotalLogs logs in ${elapsedMs}ms$printedUncleanLogDirs")
  }

  /**
   * 添加日志恢复相关的度量指标
   * 包括每个目录剩余待恢复的日志数量和每个恢复线程剩余待恢复的日志段数量
   *
   * @param numRemainingLogs 存储每个目录剩余待恢复日志数量的映射
   * @param numRemainingSegments 存储每个恢复线程剩余待恢复日志段数量的映射
   */
  private[log] def addLogRecoveryMetrics(numRemainingLogs: ConcurrentMap[String, Int],
                                         numRemainingSegments: ConcurrentMap[String, Integer]): Unit = {
    debug("Adding log recovery metrics")
    // 为每个日志目录添加度量指标
    for (dir <- logDirs) {
      // 添加剩余待恢复日志数量的度量指标
      metricsGroup.newGauge("remainingLogsToRecover", () => numRemainingLogs.get(dir.getAbsolutePath),
        Map("dir" -> dir.getAbsolutePath).asJava)
      // 为每个恢复线程添加剩余待恢复日志段数量的度量指标
      for (i <- 0 until numRecoveryThreadsPerDataDir) {
        val threadName = logRecoveryThreadName(dir.getAbsolutePath, i)
        metricsGroup.newGauge("remainingSegmentsToRecover", () => numRemainingSegments.get(threadName),
          Map("dir" -> dir.getAbsolutePath, "threadNum" -> i.toString).asJava)
      }
    }
  }
  }

  /**
   * 移除日志恢复相关的度量指标
   * 这个方法用于清理在日志恢复过程中使用的监控指标
   * 包括每个目录中待恢复的日志数量和每个恢复线程中待恢复的日志段数量
   */
  private[log] def removeLogRecoveryMetrics(): Unit = {
    debug("Removing log recovery metrics") // 记录调试信息，表示开始移除日志恢复度量指标
    for (dir <- logDirs) { // 遍历所有日志目录
      // 移除该目录下待恢复日志数量的度量指标
      metricsGroup.removeMetric("remainingLogsToRecover", Map("dir" -> dir.getAbsolutePath).asJava)
      // 遍历该目录下的所有恢复线程
      for (i <- 0 until numRecoveryThreadsPerDataDir) {
        // 移除每个恢复线程中待恢复日志段数量的度量指标
        metricsGroup.removeMetric("remainingSegmentsToRecover", Map("dir" -> dir.getAbsolutePath, "threadNum" -> i.toString).asJava)
      }
    }
  }

  /**
   * 启动后台线程以执行日志刷新和清理任务
   * 这是LogManager的主要启动方法，负责初始化和启动所有必要的后台任务
   *
   * @param topicNames 需要加载的主题名称集合
   * @param isStray 用于判断日志是否为游离日志的函数，默认返回false
   */
  def startup(topicNames: Set[String], isStray: UnifiedLog => Boolean = _ => false): Unit = {
    // 确保默认配置和覆盖配置之间的一致性
    val defaultConfig = currentDefaultConfig
    // 使用默认配置和主题特定的配置覆盖启动系统
    startupWithConfigOverrides(defaultConfig, fetchTopicConfigOverrides(defaultConfig, topicNames), isStray)
  }

  /**
   * 获取主题配置的覆盖设置
   * 这个方法用于测试，它从配置仓库中获取每个主题的特定配置
   *
   * @param defaultConfig 默认的日志配置
   * @param topicNames 需要获取配置的主题名称集合
   * @return 主题名称到其特定配置的映射
   */
  private[log] def fetchTopicConfigOverrides(defaultConfig: LogConfig, topicNames: Set[String]): Map[String, LogConfig] = {
    val topicConfigOverrides = mutable.Map[String, LogConfig]() // 创建可变映射存储主题配置覆盖
    val defaultProps = defaultConfig.originals() // 获取默认配置的原始属性
    topicNames.foreach { topicName =>
      val overrides = configRepository.topicConfig(topicName) // 从配置仓库获取主题特定的配置覆盖
      // 只包含有覆盖配置的主题，以节省内存
      if (!overrides.isEmpty) {
        val logConfig = LogConfig.fromProps(defaultProps, overrides) // 合并默认配置和覆盖配置
        topicConfigOverrides(topicName) = logConfig // 将合并后的配置添加到映射中
      }
    }
    topicConfigOverrides
  }

  /**
   * 获取指定主题的日志配置
   * 如果主题有特定的配置覆盖，则返回覆盖配置；否则返回默认配置
   *
   * @param topicName 主题名称
   * @return 主题的日志配置
   */
  private def fetchLogConfig(topicName: String): LogConfig = {
    // 确保默认配置和覆盖配置之间的一致性
    val defaultConfig = currentDefaultConfig
    // 获取主题的配置覆盖，如果没有则使用默认配置
    fetchTopicConfigOverrides(defaultConfig, Set(topicName)).values.headOption.getOrElse(defaultConfig)
  }

  /**
   * 使用配置覆盖启动日志管理器
   * 这个方法用于测试，它加载日志并启动所有必要的后台任务
   *
   * @param defaultConfig 默认的日志配置
   * @param topicConfigOverrides 主题特定的配置覆盖映射
   * @param isStray 用于判断日志是否为游离日志的函数
   */
  private[log] def startupWithConfigOverrides(
    defaultConfig: LogConfig,
    topicConfigOverrides: Map[String, LogConfig],
    isStray: UnifiedLog => Boolean): Unit = {
    // 加载日志，如果之前的关闭不干净，这个过程可能会很耗时
    loadLogs(defaultConfig, topicConfigOverrides, isStray)

    /* 调度清理任务以删除旧的日志 */
    if (scheduler != null) {
      // 启动日志保留检查任务
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      scheduler.schedule("kafka-log-retention",
                         () => cleanupLogs(),
                         initialTaskDelayMs,
                         retentionCheckMs)
      // 启动日志刷新任务
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher",
                         () => flushDirtyLogs(),
                         initialTaskDelayMs,
                         flushCheckMs)
      // 启动恢复点检查点任务
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         () => checkpointLogRecoveryOffsets(),
                         initialTaskDelayMs,
                         flushRecoveryOffsetCheckpointMs)
      // 启动日志起始偏移量检查点任务
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         () => checkpointLogStartOffsets(),
                         initialTaskDelayMs,
                         flushStartOffsetCheckpointMs)
      // 启动日志删除任务（每次删除后会动态调整周期）
      scheduler.scheduleOnce("kafka-delete-logs",
                         () => deleteLogs(),
                         initialTaskDelayMs)
    }
    // 如果启用了清理器，创建并启动日志清理器
    if (cleanerConfig.enableCleaner) {
      _cleaner = new LogCleaner(cleanerConfig, liveLogDirs, currentLogs, logDirFailureChannel, time = time)
      _cleaner.startup()
    }
  }

  /**
   * 关闭所有日志
   * 这个方法执行优雅关闭，确保所有日志都被正确刷新和关闭
   *
   * @param brokerEpoch broker的纪元号，用于写入清理关闭标记文件
   */
  def shutdown(brokerEpoch: Long = -1): Unit = {
    info("Shutting down.") // 记录关闭开始的信息

    // 移除度量指标
    metricsGroup.removeMetric("OfflineLogDirectoryCount")
    for (dir <- logDirs) {
      metricsGroup.removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath).asJava)
    }

    // 创建线程池和任务集合
    val threadPools = ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // 首先停止清理器
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown(), this)
    }

    // 获取按目录分组的日志
    val localLogsByDir = logsByDir

    // 关闭每个目录中的日志
    for (dir <- liveLogDirs) {
      debug(s"Flushing and closing logs at $dir")

      // 为每个目录创建一个固定大小的线程池
      val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
        KafkaThread.nonDaemon(s"log-closing-${dir.getAbsolutePath}", _))
      threadPools.append(pool)

      // 获取该目录下的所有日志
      val logs = logsInDir(localLogsByDir, dir).values

      // 为每个日志创建关闭任务
      val jobsForDir = logs.map { log =>
        val runnable: Runnable = () => {
          // 刷新日志以确保最新的恢复点
          log.flush(true)
          log.close()
        }
        runnable
      }

      // 提交任务到线程池并保存Future对象
      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      jobs.foreachEntry { (dir, dirJobs) =>
        // 等待所有任务完成
        if (waitForAllToComplete(dirJobs,
          e => warn(s"There was an error in one of the threads during LogManager shutdown: ${e.getCause}"))) {
          val logs = logsInDir(localLogsByDir, dir)

          // 更新最后的刷新点
          debug(s"Updating recovery points at $dir")
          checkpointRecoveryOffsetsInDir(dir, logs)

          debug(s"Updating log start offsets at $dir")
          checkpointLogStartOffsetsInDir(dir, logs)

          // 为符合条件的日志目录创建清理关闭标记文件：
          // 1. 之前有清理关闭标记文件；或
          // 2. 没有清理关闭标记文件，但所有日志都在启动时完成了恢复
          val logDirAbsolutePath = dir.getAbsolutePath
          if (hadCleanShutdownFlags.getOrDefault(logDirAbsolutePath, false) ||
              loadLogsCompletedFlags.getOrDefault(logDirAbsolutePath, false)) {
            val cleanShutdownFileHandler = new CleanShutdownFileHandler(dir.getPath)
            debug(s"Writing clean shutdown marker at $dir with broker epoch=$brokerEpoch")
            CoreUtils.swallow(cleanShutdownFileHandler.write(brokerEpoch), this)
          }
        }
      }
    } finally {
      // 关闭所有线程池
      threadPools.foreach(_.shutdown())
      // 无论关闭是否成功，都需要解锁数据目录
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.") // 记录关闭完成的信息
  }

  /**
   * 将分区日志截断到指定的偏移量，并将恢复点检查点更新到该偏移量
   * 
   * 应用场景：
   * 1. 副本同步时，follower需要截断日志以匹配leader的日志
   * 2. 处理消息格式转换或日志压缩等场景下的日志重组
   * 
   * 设计考虑：
   * 1. 支持对当前日志和未来日志的截断操作
   * 2. 在截断过程中暂停日志清理以确保数据一致性
   * 3. 截断后更新检查点以支持快速恢复
   *
   * @param partitionOffsets 需要截断的分区日志及其目标偏移量的映射
   * @param isFuture 是否对指定分区的未来日志进行截断
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long], isFuture: Boolean): Unit = {
    // 创建一个数组缓冲区来存储受影响的日志
    val affectedLogs = ArrayBuffer.empty[UnifiedLog]
    // 遍历需要截断的分区及其目标偏移量
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      // 根据isFuture标志获取相应的日志对象
      val log = {
        if (isFuture)
          futureLogs.get(topicPartition)
        else
          currentLogs.get(topicPartition)
      }
      // 如果日志不存在，跳过处理
      if (log != null) {
        // 判断是否需要停止日志清理器
        // 当截断偏移量小于活动段的基础偏移量时，需要停止清理器
        val needToStopCleaner = truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && !isFuture)
          abortAndPauseCleaning(topicPartition)
        try {
          // 执行日志截断操作，如果截断成功则将日志添加到受影响列表
          if (log.truncateTo(truncateOffset))
            affectedLogs += log
          // 如果需要，更新清理器检查点到活动段的基础偏移量
          if (needToStopCleaner && !isFuture)
            maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
        } finally {
          // 恢复日志清理操作
          if (needToStopCleaner && !isFuture)
            resumeCleaning(topicPartition)
        }
      }
    }

    // 为所有受影响的日志目录更新恢复点检查点
    for (dir <- affectedLogs.map(_.parentDirFile).distinct) {
      checkpointRecoveryOffsetsInDir(dir)
    }
  }

  /**
   * 删除分区中的所有数据并从新的偏移量开始日志
   * 
   * 应用场景：
   * 1. 主题分区重新分配时的日志清理
   * 2. 删除主题时的数据清理
   * 3. 日志格式升级时的完全重建
   * 
   * 设计考虑：
   * 1. 支持指定新的起始偏移量
   * 2. 可选择性地设置日志起始偏移量
   * 3. 确保在清理过程中的数据一致性
   *
   * @param topicPartition 需要截断的分区
   * @param newOffset 日志的新起始偏移量
   * @param isFuture 是否对指定分区的未来日志进行操作
   * @param logStartOffsetOpt 日志的起始偏移量，如果为None则使用newOffset
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition,
                              newOffset: Long,
                              isFuture: Boolean,
                              logStartOffsetOpt: Option[Long] = None): Unit = {
    // 根据isFuture标志获取相应的日志对象
    val log = {
      if (isFuture)
        futureLogs.get(topicPartition)
      else
        currentLogs.get(topicPartition)
    }
    // 如果日志存在则进行处理
    if (log != null) {
      // 如果不是未来日志，暂停日志清理
      if (!isFuture)
        abortAndPauseCleaning(topicPartition)
      try {
        // 执行完全截断操作并设置新的起始偏移量
        log.truncateFullyAndStartAt(newOffset, logStartOffsetOpt)
        // 如果需要，更新清理器检查点
        if (!isFuture)
          maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
      } finally {
        // 恢复日志清理操作
        if (!isFuture)
          resumeCleaning(topicPartition)
      }
      // 更新日志目录的恢复点检查点
      checkpointRecoveryOffsetsInDir(log.parentDirFile)
    }
  }

  /**
   * 将所有日志的当前恢复点写入日志目录中的文本文件
   * 这样可以避免在启动时恢复整个日志
   * 
   * 应用场景：
   * 1. Broker正常关闭时保存恢复点信息
   * 2. 定期检查点以支持快速恢复
   * 
   * 设计考虑：
   * 1. 按目录组织检查点信息
   * 2. 确保检查点信息的持久化
   */
  def checkpointLogRecoveryOffsets(): Unit = {
    // 获取按目录分组的日志缓存
    val logsByDirCached = logsByDir
    // 遍历所有活跃的日志目录
    liveLogDirs.foreach { logDir =>
      // 获取目录中需要检查点的日志
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      // 执行检查点操作
      checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
    }
  }

  /**
   * 将所有日志的当前起始偏移量写入日志目录中的文本文件
   * 这样可以避免暴露已被DeleteRecordsRequest删除的数据
   * 
   * 应用场景：
   * 1. 记录删除请求后的日志起始位置
   * 2. 支持日志压缩和清理操作
   * 
   * 设计考虑：
   * 1. 保护已删除的数据不被访问
   * 2. 维护日志段的有效范围
   */
  def checkpointLogStartOffsets(): Unit = {
    // 获取按目录分组的日志缓存
    val logsByDirCached = logsByDir
    // 遍历所有活跃的日志目录
    liveLogDirs.foreach { logDir =>
      // 执行起始偏移量的检查点操作
      checkpointLogStartOffsetsInDir(logDir, logsInDir(logsByDirCached, logDir))
    }
  }

  /**
   * 为指定日志目录中的所有日志创建恢复点检查点
   * 仅用于测试目的
   *
   * @param logDir 需要创建检查点的日志目录
   */
  private[log] def checkpointRecoveryOffsetsInDir(logDir: File): Unit = {
    checkpointRecoveryOffsetsInDir(logDir, logsInDir(logDir))
  }

  /**
   * 为提供的所有日志创建恢复点检查点
   * 
   * 设计考虑：
   * 1. 使用原子写入确保检查点文件的一致性
   * 2. 处理存储异常并将故障目录标记为离线
   *
   * @param logDir 日志所在的目录
   * @param logsToCheckpoint 需要创建检查点的日志映射
   */
  private def checkpointRecoveryOffsetsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, UnifiedLog]): Unit = {
    try {
      // 获取目录对应的检查点对象并执行检查点操作
      recoveryPointCheckpoints.get(logDir).foreach { checkpoint =>
        // 创建恢复偏移量映射，将Scala的Long转换为Java的Long
        val recoveryOffsets: Map[TopicPartition, JLong] = logsToCheckpoint.map { case (tp, log) => tp -> long2Long(log.recoveryPoint) }
        // 使用原子移动操作写入检查点文件，确保崩溃一致性
        checkpoint.write(recoveryOffsets.asJava)
      }
    } catch {
      case e: KafkaStorageException =>
        // 记录磁盘错误
        error(s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}")
      case e: IOException =>
        // 将目录标记为离线并记录错误信息
        logDirFailureChannel.maybeAddOfflineLogDir(logDir.getAbsolutePath,
          s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}", e)
    }
  }

  /**
   * 为指定目录中的所有日志写入日志起始偏移量检查点。
   * 这个方法用于持久化保存每个分区的日志起始偏移量，以便在broker重启时能够恢复到正确的位置。
   * 只有启用了远程日志存储或日志起始偏移量大于第一个日志段的基准偏移量的分区才会被记录。
   *
   * @param logDir 需要写入检查点的目录
   * @param logsToCheckpoint 需要写入检查点的日志映射表
   */
  private def checkpointLogStartOffsetsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, UnifiedLog]): Unit = {
    try {
      // 获取目录对应的检查点对象并执行写入操作
      logStartOffsetCheckpoints.get(logDir).foreach { checkpoint =>
        // 收集需要记录检查点的日志起始偏移量
        val logStartOffsets: Map[TopicPartition, JLong] = logsToCheckpoint.collect {
          // 只记录启用了远程日志或起始偏移量大于第一个日志段基准偏移量的分区
          case (tp, log) if log.remoteLogEnabled() || log.logStartOffset > log.logSegments.asScala.head.baseOffset =>
            tp -> long2Long(log.logStartOffset)
        }
        // 将检查点数据写入文件
        checkpoint.write(logStartOffsets.asJava)
      }
    } catch {
      case e: KafkaStorageException =>
        error(s"写入日志起始偏移量检查点到目录 $logDir 时发生磁盘错误: ${e.getMessage}")
    }
  }

  /**
   * 更新主题分区的首选日志目录。
   * 只有当指定目录中不存在该分区的当前日志和未来日志时，才会更新首选目录。
   * 这个方法用于在日志目录迁移或重新平衡时指定分区的目标目录。
   *
   * @param topicPartition 主题分区
   * @param logDir 首选日志目录的绝对路径
   */
  def maybeUpdatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // 如果指定目录中不存在该分区的当前日志和未来日志，则更新首选目录
    if (!getLog(topicPartition).exists(_.parentDir == logDir) &&
        !getLog(topicPartition, isFuture = true).exists(_.parentDir == logDir))
      preferredLogDirs.put(topicPartition, logDir)
  }

  /**
   * 中止并暂停指定分区的日志清理操作。
   * 这个方法通常在需要临时停止日志压缩时使用，例如在配置更新或维护期间。
   *
   * @param topicPartition 需要暂停清理的主题分区
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      // 通知清理器中止并暂停指定分区的清理操作
      cleaner.abortAndPauseCleaning(topicPartition)
      info(s"分区 $topicPartition 的清理操作已中止并暂停")
    }
  }

  /**
   * 中止指定分区的日志清理操作。
   * 与abortAndPauseCleaning不同，这个方法只中止当前的清理操作，不会暂停后续的清理。
   *
   * @param topicPartition 需要中止清理的主题分区
   */
  def abortCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      // 通知清理器中止指定分区的清理操作
      cleaner.abortCleaning(topicPartition)
      info(s"分区 $topicPartition 的清理操作已中止")
    }
  }

  /**
   * 恢复指定分区的日志清理操作。
   * 这个方法用于重新启动之前被暂停的日志清理操作。
   *
   * @param topicPartition 需要恢复清理的主题分区
   */
  private def resumeCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      // 通知清理器恢复指定分区的清理操作
      cleaner.resumeCleaning(Seq(topicPartition))
      info(s"分区 $topicPartition 的清理操作已恢复")
    }
  }

  /**
   * 将清理器的检查点截断到指定日志的活动段基准偏移量。
   * 这个操作通常在日志截断后执行，以确保清理器的检查点与实际日志内容保持一致。
   *
   * @param log 需要截断检查点的日志
   * @param topicPartition 日志所属的主题分区
   */
  private def maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log: UnifiedLog, topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      // 将清理器检查点截断到活动段的基准偏移量
      cleaner.maybeTruncateCheckpoint(log.parentDirFile, topicPartition, log.activeSegment.baseOffset)
    }
  }

  /**
   * 获取指定主题分区的日志对象。
   * 支持获取当前日志或未来日志（用于日志目录迁移）。
   *
   * @param topicPartition 主题分区
   * @param isFuture 如果为true，返回未来日志；否则返回当前日志
   * @return 如果日志存在则返回Some(日志对象)，否则返回None
   */
  def getLog(topicPartition: TopicPartition, isFuture: Boolean = false): Option[UnifiedLog] = {
    if (isFuture)
      // 从未来日志池中获取日志
      Option(futureLogs.get(topicPartition))
    else
      // 从当前日志池中获取日志
      Option(currentLogs.get(topicPartition))
  }

  /**
   * 标记指定分区的日志正在初始化。
   * 这个方法必须与finishedInitializingLog配对使用，以正确管理日志的初始化状态。
   * 初始化状态用于跟踪日志加载过程，确保配置更新能够正确应用。
   *
   * @param topicPartition 正在初始化的主题分区
   */
  def initializingLog(topicPartition: TopicPartition): Unit = {
    // 将分区的初始化状态设置为false，表示正在初始化
    partitionsInitializing(topicPartition) = false
  }

  /**
   * 将指定主题所有正在初始化的分区配置标记为脏。
   * 这将导致这些分区在初始化完成后重新加载配置。
   * 这个机制确保了配置更新能够正确应用到正在初始化的分区。
   *
   * @param topic 需要更新配置的主题
   */
  def topicConfigUpdated(topic: String): Unit = {
    // 找到所有属于该主题且正在初始化的分区，将其配置标记为脏（true）
    partitionsInitializing.keys.filter(_.topic() == topic).foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * 更新指定主题的配置。
   * 这个方法处理主题级别的配置更新，包括远程存储和日志压缩等设置的变更。
   * 会验证配置的有效性，并确保配置更改不会导致不一致的状态。
   *
   * @param topic 需要更新配置的主题
   * @param newTopicConfig 新的主题配置
   * @param isRemoteLogStorageSystemEnabled 是否启用了远程日志存储系统
   * @param wasRemoteLogEnabled 之前是否启用了远程日志存储
   */
  def updateTopicConfig(topic: String,
                        newTopicConfig: Properties,
                        isRemoteLogStorageSystemEnabled: Boolean,
                        wasRemoteLogEnabled: Boolean): Unit = {
    // 标记主题配置已更新，确保正在初始化的分区会重新加载配置
    topicConfigUpdated(topic)
    // 获取主题的所有日志
    val logs = logsByTopic(topic)
    // 从属性创建新的日志配置
    val newLogConfig = LogConfig.fromProps(currentDefaultConfig.originals, newTopicConfig)
    val isRemoteLogStorageEnabled = newLogConfig.remoteStorageEnable()
    
    // 验证远程存储相关的配置
    // 即使日志尚未在磁盘上实现，也要验证配置
    // 这样可以防止在禁用分层存储时创建分层主题导致的问题
    LogConfig.validateRemoteStorageOnlyIfSystemEnabled(newLogConfig.values(), isRemoteLogStorageSystemEnabled, true)
    LogConfig.validateTurningOffRemoteStorageWithDelete(newLogConfig.values(), wasRemoteLogEnabled, isRemoteLogStorageEnabled)
    LogConfig.validateRetentionConfigsWhenRemoteCopyDisabled(newLogConfig.values(), isRemoteLogStorageEnabled)
    
    // 如果主题有日志，更新每个日志的配置
    if (logs.nonEmpty) {
      logs.foreach { log =>
        // 更新日志配置并获取旧配置
        val oldLogConfig = log.updateConfig(newLogConfig)
        // 如果日志压缩被禁用，中止正在进行的清理操作
        if (oldLogConfig.compact && !newLogConfig.compact) {
          abortCleaning(log.topicPartition)
        }
      }
    }
  }

  /**
   * 当broker配置更新时，将所有正在初始化的分区配置标记为脏。
   * 这确保了broker级别的配置更改能够正确传播到所有正在初始化的分区。
   */
  def brokerConfigUpdated(): Unit = {
    // 将所有正在初始化的分区的配置标记为脏（true）
    partitionsInitializing.keys.foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * 表示指定分区的日志初始化已完成的方法。该方法应该在调用[[kafka.log.LogManager#initializingLog]]之后执行。
   * 
   * 如果在日志加载过程中主题配置发生了更新，该方法会再次获取主题配置并应用更新。
   * 
   * @param topicPartition 已完成初始化的主题分区
   * @param maybeLog 可能存在的统一日志对象
   */
  def finishedInitializingLog(topicPartition: TopicPartition,
                              maybeLog: Option[UnifiedLog]): Unit = {
    // 从正在初始化的分区映射中移除该分区，并获取其标记值
    val removedValue = partitionsInitializing.remove(topicPartition)
    // 如果标记值为true，表示在初始化过程中配置发生了更新
    // 此时需要重新获取最新的配置并更新日志配置
    if (removedValue.contains(true))
      maybeLog.foreach(_.updateConfig(fetchLogConfig(topicPartition.topic)))
  }

  /**
   * 获取或创建指定主题分区的日志。
   * 如果日志已存在，则返回现有日志的副本；
   * 如果isNew=true或没有离线日志目录，则为指定的主题和分区创建新日志；
   * 否则抛出KafkaStorageException异常。
   *
   * @param topicPartition 需要返回或创建日志的分区
   * @param isNew 副本是否应该已经存在于broker上
   * @param isFuture 是否返回或创建指定分区的未来日志
   * @param topicId 分区所属主题的ID
   * @param targetLogDirectoryId 应该托管分区主题的目录ID。
   *                             如果为None或等于{@link DirectoryId.UNASSIGNED}，将选择下一个可用目录。
   *                             该方法假设提供的ID属于在线目录。
   * @throws KafkaStorageException 当isNew=false且日志不在缓存中且broker上有离线日志目录时抛出
   * @throws InconsistentTopicIdException 当日志中的主题ID与提供的主题ID不匹配时抛出
   */
  def getOrCreateLog(topicPartition: TopicPartition, isNew: Boolean = false, isFuture: Boolean = false,
                     topicId: Option[Uuid], targetLogDirectoryId: Option[Uuid] = Option.empty): UnifiedLog = {
    // 使用同步锁确保日志创建或删除的原子性
    logCreationOrDeletionLock synchronized {
      // 尝试获取现有日志，如果不存在则创建新日志
      val log = getLog(topicPartition, isFuture).getOrElse {
        // 如果不是新建且存在离线日志目录，则抛出异常
        if (!isNew && offlineLogDirs.nonEmpty)
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")

        // 确定日志目录
        val logDirs: List[File] = {
          // 根据目标目录ID和首选目录配置确定首选日志目录
          val preferredLogDir = targetLogDirectoryId.filterNot(Seq(DirectoryId.UNASSIGNED,DirectoryId.LOST).contains) match {
            case Some(targetId) if !preferredLogDirs.containsKey(topicPartition) =>
              // 如果分区同时配置了targetLogDirectoryId和preferredLogDirs
              // 则优先使用preferredLogDirs，否则使用targetLogDirectoryId
              directoryIds.find(_._2 == targetId).map(_._1).orNull
            case _ =>
              preferredLogDirs.get(topicPartition)
          }

          // 处理未来日志的特殊情况
          if (isFuture) {
            // 未来日志必须有首选目录
            if (preferredLogDir == null)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition without having a preferred log directory")
            // 未来日志不能与当前日志使用相同目录
            else if (getLog(topicPartition).get.parentDir == preferredLogDir)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition in the current log directory of this partition")
          }

          // 如果有首选目录则使用它，否则选择下一个可用目录
          if (preferredLogDir != null)
            List(new File(preferredLogDir))
          else
            nextLogDirs()
        }

        // 根据是否为未来日志确定日志目录名
        val logDirName = {
          if (isFuture)
            UnifiedLog.logFutureDirName(topicPartition)
          else
            UnifiedLog.logDirName(topicPartition)
        }

        // 创建日志目录，使用迭代器避免一次性映射整个列表
        val logDir = logDirs
          .iterator
          .map(createLogDirectory(_, logDirName))
          .find(_.isSuccess)
          .getOrElse(Failure(new KafkaStorageException("No log directories available. Tried " + logDirs.map(_.getAbsolutePath).mkString(", "))))
          .get // 如果失败则抛出异常

        // 获取日志配置并创建统一日志对象
        val config = fetchLogConfig(topicPartition.topic)
        val log = UnifiedLog(
          dir = logDir,
          config = config,
          logStartOffset = 0L,
          recoveryPoint = 0L,
          maxTransactionTimeoutMs = maxTransactionTimeoutMs,
          producerStateManagerConfig = producerStateManagerConfig,
          producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
          scheduler = scheduler,
          time = time,
          brokerTopicStats = brokerTopicStats,
          logDirFailureChannel = logDirFailureChannel,
          topicId = topicId,
          remoteStorageSystemEnable = remoteStorageSystemEnable)

        // 将日志添加到相应的日志池中
        if (isFuture)
          futureLogs.put(topicPartition, log)
        else
          currentLogs.put(topicPartition, log)

        // 记录日志创建信息
        info(s"Created log for partition $topicPartition in $logDir with properties ${config.overriddenConfigsAsLoggableString}")
        // 移除已满足的首选日志目录
        preferredLogDirs.remove(topicPartition)

        log
      }
      
      // 确保主题ID一致性
      topicId.foreach { topicId =>
        log.topicId.foreach { logTopicId =>
          if (topicId != logTopicId)
            throw new InconsistentTopicIdException(s"Tried to assign topic ID $topicId to log for topic partition $topicPartition," +
              s"but log already contained topic ID $logTopicId")
        }
      }
      log
    }
  }

  /**
   * 在指定的日志目录中创建日志子目录
   * 
   * @param logDir 父日志目录
   * @param logDirName 要创建的日志目录名
   * @return 成功则返回Success(创建的目录)，失败则返回Failure(异常)
   */
  private[log] def createLogDirectory(logDir: File, logDirName: String): Try[File] = {
    // 获取日志目录的绝对路径
    val logDirPath = logDir.getAbsolutePath
    // 检查日志目录是否在线
    if (isLogDirOnline(logDirPath)) {
      // 创建新的日志目录对象
      val dir = new File(logDirPath, logDirName)
      try {
        // 创建目录及其所有必需的父目录
        Files.createDirectories(dir.toPath)
        Success(dir)
      } catch {
        case e: IOException =>
          // 创建目录失败时的错误处理
          val msg = s"Error while creating log for $logDirName in dir $logDirPath"
          // 将目录添加到离线目录通道
          logDirFailureChannel.maybeAddOfflineLogDir(logDirPath, msg, e)
          // 记录警告日志
          warn(msg, e)
          // 返回存储异常
          Failure(new KafkaStorageException(msg, e))
      }
    } else {
      // 如果日志目录离线，返回存储异常
      Failure(new KafkaStorageException(s"Can not create log $logDirName because log directory $logDirPath is offline"))
    }
  }

  /**
   * 删除已标记为待删除的日志。删除所有已经超过`currentDefaultConfig.fileDeleteDelayMs`延迟时间的日志。
   * 对于尚未超过此时间间隔的日志，将在下一次`deleteLogs`迭代中考虑删除。
   * 下一次迭代将在第一个未删除日志的剩余时间后执行。
   * 如果没有更多的`logsToBeDeleted`，`deleteLogs`将在`max(currentDefaultConfig.fileDeleteDelayMs, 1)`后执行。
   */
  private def deleteLogs(): Unit = {
    // 下一次删除操作的延迟时间
    var nextDelayMs = 0L
    // 获取文件删除延迟配置
    val fileDeleteDelayMs = currentDefaultConfig.fileDeleteDelayMs
    try {
      // 计算下一次删除操作的延迟时间
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          // 获取队列头部日志的调度时间，计算剩余延迟时间
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          scheduleTimeMs + fileDeleteDelayMs - time.milliseconds()
        } else {
          // 避免fileDeleteDelayMs为0且logsToBeDeleted为空的情况
          // 在这种情况下，logsToBeDeleted.take()将永远阻塞
          Math.max(fileDeleteDelayMs, 1)
        }
      }

      // 当有日志可以删除时（延迟时间已到），执行删除操作
      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        // 从队列中取出要删除的日志
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            // 执行日志删除操作
            removedLog.delete()
            // 记录删除成功信息
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              // 记录删除失败错误
              error(s"Exception while deleting $removedLog in dir ${removedLog.parentDir}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        // 记录删除线程异常
        error(s"Exception in kafka-delete-logs thread.", e)
    } finally {
      try {
        // 调度下一次删除操作
        scheduler.scheduleOnce("kafka-delete-logs",
          () => deleteLogs(),
          nextDelayMs)
      } catch {
        case e: Throwable =>
          // 除非调度器已关闭，否则不应发生错误
          error(s"Failed to schedule next delete in kafka-delete-logs thread", e)
      }
    }
  }

  /**
   * 恢复被遗弃的未来日志。在Kafka集群中，当分区的副本从一个目录移动到另一个目录时，会创建未来日志。
   * 如果在移动过程中发生故障，这些未来日志可能被遗弃。此方法用于恢复这些被遗弃的日志。
   *
   * @param brokerId 当前broker的ID
   * @param newTopicsImage 最新的主题元数据镜像
   */
  def recoverAbandonedFutureLogs(brokerId: Int, newTopicsImage: TopicsImage): Unit = {
    // 查找所有被遗弃的未来日志
    val abandonedFutureLogs = findAbandonedFutureLogs(brokerId, newTopicsImage)
    // 遍历每个被遗弃的未来日志
    abandonedFutureLogs.foreach { case (futureLog, currentLog) =>
      val tp = futureLog.topicPartition
      // 中止并暂停日志清理，因为日志清理器是异步运行的，而replaceCurrentWithFutureLog
      // 会调用resumeCleaning，这需要日志清理器的内部状态中包含给定主题分区的键
      abortAndPauseCleaning(tp)

      // 记录恢复操作的开始
      if (currentLog.isDefined)
        info(s"Attempting to recover abandoned future log for $tp at $futureLog and removing ${currentLog.get}")
      else
        info(s"Attempting to recover abandoned future log for $tp at $futureLog")
      // 用未来日志替换当前日志
      replaceCurrentWithFutureLog(currentLog, futureLog)
      // 记录恢复操作的完成
      info(s"Successfully recovered abandoned future log for $tp")
    }
  }

  /**
   * 查找被遗弃的未来日志。一个未来日志被认为是被遗弃的，如果它满足以下条件：
   * 1. 存在于futureLogs中
   * 2. 具有有效的主题ID
   * 3. 其目录ID与主题元数据中指定的目录ID匹配
   *
   * @param brokerId 当前broker的ID
   * @param newTopicsImage 最新的主题元数据镜像
   * @return 返回被遗弃的未来日志及其对应的当前日志（如果存在）的集合
   */
  private def findAbandonedFutureLogs(brokerId: Int, newTopicsImage: TopicsImage): Iterable[(UnifiedLog, Option[UnifiedLog])] = {
    // 遍历所有未来日志
    futureLogs.values.flatMap { futureLog =>
      // 获取主题ID，如果不存在则抛出异常（在KRaft模式下必须有主题ID）
      val topicId = futureLog.topicId.getOrElse {
        throw new RuntimeException(s"The log dir $futureLog does not have a topic ID, " +
          "which is not allowed when running in KRaft mode.")
      }
      // 获取分区ID
      val partitionId = futureLog.topicPartition.partition()
      // 检查主题元数据中是否存在该分区，并验证目录ID是否匹配
      Option(newTopicsImage.getPartition(topicId, partitionId))
        .filter(pr => directoryId(futureLog.parentDir).contains(pr.directory(brokerId)))
        .map(_ => (futureLog, Option(currentLogs.get(futureLog.topicPartition)).filter(currentLog => currentLog.topicId.contains(topicId))))
    }
  }

  /**
   * 将源日志目录中的分区目录标记为待删除，并将目标日志目录中的未来日志重命名为当前日志。
   * 这个方法用于完成日志目录迁移过程。
   *
   * @param topicPartition 需要进行交换的主题分区
   */
  def replaceCurrentWithFutureLog(topicPartition: TopicPartition): Unit = {
    // 使用同步锁确保日志创建和删除操作的原子性
    logCreationOrDeletionLock synchronized {
      // 获取当前日志和未来日志
      val sourceLog = currentLogs.get(topicPartition)
      val destLog = futureLogs.get(topicPartition)

      // 如果任一日志不存在，抛出存储异常
      if (sourceLog == null)
        throw new KafkaStorageException(s"The current replica for $topicPartition is offline")
      if (destLog == null)
        throw new KafkaStorageException(s"The future replica for $topicPartition is offline")

      // 记录替换操作的开始和完成
      info(s"Attempting to replace current log $sourceLog with $destLog for $topicPartition")
      replaceCurrentWithFutureLog(Option(sourceLog), destLog, updateHighWatermark = true)
      info(s"The current replica is successfully replaced with the future replica for $topicPartition")
    }
  }

  /**
   * 执行日志替换操作的具体实现。
   * 此方法负责：
   * 1. 重命名目录
   * 2. 更新度量指标
   * 3. 更新高水位标记
   * 4. 更新缓存映射和日志清理器状态
   * 5. 处理源日志的清理和删除
   *
   * @param sourceLog 源日志（当前日志）
   * @param destLog 目标日志（未来日志）
   * @param updateHighWatermark 是否需要更新高水位标记
   */
  def replaceCurrentWithFutureLog(sourceLog: Option[UnifiedLog], destLog: UnifiedLog, updateHighWatermark: Boolean = false): Unit = {
    val topicPartition = destLog.topicPartition

    // 将目标日志重命名为当前日志的目录名
    destLog.renameDir(UnifiedLog.logDirName(topicPartition), shouldReinitialize = true)
    // 移除带有"future"标记的度量指标
    destLog.removeLogMetrics()
    // 如果需要，更新高水位标记
    if (updateHighWatermark && sourceLog.isDefined) {
      destLog.updateHighWatermark(sourceLog.get.highWatermark)
    }

    // 更新缓存映射和日志清理器状态
    futureLogs.remove(topicPartition)
    currentLogs.put(topicPartition, destLog)
    if (cleaner != null) {
      sourceLog.foreach { srcLog =>
        cleaner.alterCheckpointDir(topicPartition, srcLog.parentDirFile, destLog.parentDirFile)
      }
      resumeCleaning(topicPartition)
    }

    try {
      // 处理源日志：重命名、关闭、更新检查点并加入删除队列
      sourceLog.foreach { srcLog =>
        srcLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition), shouldReinitialize = true)
        srcLog.close()
        val logDir = srcLog.parentDirFile
        val logsToCheckpoint = logsInDir(logDir)
        checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
        checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        srcLog.removeLogMetrics()
        addLogToBeDeleted(srcLog)
      }
      // 为目标日志创建新的度量指标
      destLog.newMetrics()
    } catch {
      case e: KafkaStorageException =>
        // 如果源日志目录离线，需要在这里关闭其处理器
        // 因为handleLogDirFailure()不会关闭已从currentLogs映射中移除的源日志的处理器
        sourceLog.foreach { srcLog =>
          srcLog.closeHandlers()
          srcLog.removeLogMetrics()
        }
        throw e
    }
  }

  /**
   * 将指定主题分区的日志目录重命名为"logdir.uuid.delete"并将其加入删除队列。
   * 此方法支持异步删除日志，可以处理当前日志、未来日志和游离日志。
   *
   * @param topicPartition 需要删除的主题分区
   * @param isFuture 如果为true，表示要删除指定分区的未来日志
   * @param checkpoint 如果为true，表示需要写入检查点
   * @param isStray 如果为true，表示这是一个游离日志，只需移动而不删除
   * @return 返回被移除的日志对象
   */
  def asyncDelete(topicPartition: TopicPartition,
                  isFuture: Boolean = false,
                  checkpoint: Boolean = true,
                  isStray: Boolean = false): Option[UnifiedLog] = {
    // 同步移除日志和相关度量指标
    val removedLog: Option[UnifiedLog] = logCreationOrDeletionLock synchronized {
      removeLogAndMetrics(if (isFuture) futureLogs else currentLogs, topicPartition)
    }
    removedLog match {
      case Some(removedLog) =>
        // 在实际删除之前，需要等待没有正在进行的清理任务
        if (cleaner != null && !isFuture) {
          cleaner.abortCleaning(topicPartition)
          if (checkpoint) {
            cleaner.updateCheckpoints(removedLog.parentDirFile, partitionToRemove = Option(topicPartition))
          }
        }
        if (isStray) {
          // 对于游离分区，只移动不删除
          removedLog.renameDir(UnifiedLog.logStrayDirName(topicPartition), shouldReinitialize = false)
          warn(s"Log for partition ${removedLog.topicPartition} is marked as stray and renamed to ${removedLog.dir.getAbsolutePath}")
        } else {
          // 重命名日志目录并加入删除队列
          removedLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition), shouldReinitialize = false)
          addLogToBeDeleted(removedLog)
          info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")
        }
        // 如果需要，更新检查点
        if (checkpoint) {
          val logDir = removedLog.parentDirFile
          val logsToCheckpoint = logsInDir(logDir)
          checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
          checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        }

      case None =>
        // 如果有离线的日志目录，可能日志在其中，此时抛出存储异常
        if (offlineLogDirs.nonEmpty) {
          throw new KafkaStorageException(s"Failed to delete log for ${if (isFuture) "future" else ""} $topicPartition because it may be in one of the offline directories ${offlineLogDirs.mkString(",")}")
        }
    }

    removedLog
  }

  /**
   * 重命名给定主题分区的目录，并将它们添加到删除队列中。
   * 一旦所有目录都被重命名，检查点信息会被更新。
   * 这个方法实现了日志的异步删除功能，主要用于以下场景：
   * 1. 删除主题时的日志清理
   * 2. 分区重分配后的旧日志清理
   * 3. 处理游离日志（不再属于任何活跃主题的日志）
   *
   * @param topicPartitions 需要异步删除的主题分区集合
   * @param isStray 是否为游离日志
   * @param errorHandler 当特定主题分区发生异常时调用的错误处理函数
   */
  def asyncDelete(topicPartitions: Iterable[TopicPartition],
                  isStray: Boolean,
                  errorHandler: (TopicPartition, Throwable) => Unit): Unit = {
    // 创建一个空的Set用于存储需要更新检查点的日志目录
    val logDirs = mutable.Set.empty[File]

    // 遍历每个需要删除的主题分区
    topicPartitions.foreach { topicPartition =>
      try {
        // 处理当前日志
        getLog(topicPartition).foreach { log =>
          logDirs += log.parentDirFile // 记录日志所在目录
          asyncDelete(topicPartition, checkpoint = false, isStray = isStray) // 执行异步删除
        }
        // 处理未来日志（如果存在）
        getLog(topicPartition, isFuture = true).foreach { log =>
          logDirs += log.parentDirFile // 记录未来日志所在目录
          asyncDelete(topicPartition, isFuture = true, checkpoint = false, isStray = isStray) // 执行异步删除
        }
      } catch {
        case e: Throwable => errorHandler(topicPartition, e) // 发生异常时调用错误处理函数
      }
    }

    // 获取日志目录映射的缓存，避免重复计算
    val logsByDirCached = logsByDir
    // 更新每个受影响目录的检查点信息
    logDirs.foreach { logDir =>
      if (cleaner != null) cleaner.updateCheckpoints(logDir) // 更新清理器检查点
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir) // 获取目录中的日志
      checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint) // 更新恢复点检查点
      checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint) // 更新起始偏移量检查点
    }
  }

  /**
   * 提供下一个分区的建议目录的完整有序列表。
   * 当前的实现方式是计算每个目录中的分区数量，然后按分区数量最少的顺序排序。
   * 这种策略有助于实现以下目标：
   * 1. 在多个日志目录之间均衡分布分区
   * 2. 优先使用负载较轻的目录
   * 3. 避免单个目录过载
   */
  private def nextLogDirs(): List[File] = {
    // 如果只有一个活跃的日志目录，直接返回该目录
    if (_liveLogDirs.size == 1) {
      List(_liveLogDirs.peek())
    } else {
      // 统计每个父目录中的日志数量（包括空目录的0计数）
      val logCounts = allLogs.groupBy(_.parentDir).map { case (parent, logs) => parent -> logs.size }
      // 为所有活跃目录创建初始计数为0的映射
      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
      // 合并实际日志计数和初始计数
      val dirCounts = (zeros ++ logCounts).toBuffer

      // 选择日志数量最少的目录，按日志数量升序排序
      dirCounts.sortBy(_._2).map {
        case (path: String, _: Int) => new File(path)
      }.toList
    }
  }

  /**
   * 删除所有符合条件的日志，返回删除的日志段数量。
   * 只考虑未启用压缩的日志。
   * 此方法主要用于：
   * 1. 定期清理过期的日志段
   * 2. 执行日志保留策略
   * 3. 释放磁盘空间
   */
  private def cleanupLogs(): Unit = {
    debug("开始日志清理...")
    var total = 0 // 记录删除的日志段总数
    val startMs = time.milliseconds // 记录开始时间

    // 获取可删除的日志
    val deletableLogs = {
      if (cleaner != null) {
        // 暂停清理器对非压缩分区的清理工作，避免并发操作
        cleaner.pauseCleaningForNonCompactedPartitions()
      } else {
        // 如果没有清理器，只选择未启用压缩的日志
        currentLogs.filter {
          case (_, log) => !log.config.compact
        }
      }
    }

    try {
      // 遍历可删除的日志执行清理
      deletableLogs.foreach {
        case (topicPartition, log) =>
          debug(s"正在进行垃圾回收 '${log.name}'")
          total += log.deleteOldSegments() // 删除当前日志的旧段

          // 处理未来日志（如果存在）
          val futureLog = futureLogs.get(topicPartition)
          if (futureLog != null) {
            debug(s"正在进行未来日志垃圾回收 '${futureLog.name}'")
            total += futureLog.deleteOldSegments() // 删除未来日志的旧段
          }
      }
    } finally {
      // 恢复清理器的工作
      if (cleaner != null) {
        cleaner.resumeCleaning(deletableLogs.map(_._1))
      }
    }

    debug(s"日志清理完成。共删除 $total 个文件，耗时 " +
                  (time.milliseconds - startMs) / 1000 + " 秒")
  }

  /**
   * 获取所有分区的日志
   * 返回当前日志和未来日志的组合集合
   */
  def allLogs: Iterable[UnifiedLog] = currentLogs.values ++ futureLogs.values

  /**
   * 获取指定主题的所有日志
   * 
   * @param topic 主题名称
   * @return 该主题的所有日志列表
   */
  def logsByTopic(topic: String): Seq[UnifiedLog] = {
    (currentLogs.toList ++ futureLogs.toList).collect {
      case (topicPartition, log) if topicPartition.topic == topic => log
    }
  }

  /**
   * 获取每个日志目录中的主题分区日志映射
   * 此方法经过优化，减少了在处理大量主题分区时的内存分配和CPU使用
   * 主要用于检查点处理，需要确保性能高效
   */
  private def logsByDir: Map[String, Map[TopicPartition, UnifiedLog]] = {
    // 使用AnyRefMap减少内存分配，提高性能
    val byDir = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, UnifiedLog]]()
    // 将日志添加到对应目录的映射中
    def addToDir(tp: TopicPartition, log: UnifiedLog): Unit = {
      byDir.getOrElseUpdate(log.parentDir, new mutable.AnyRefMap[TopicPartition, UnifiedLog]()).put(tp, log)
    }
    // 处理当前日志和未来日志
    currentLogs.foreachEntry(addToDir)
    futureLogs.foreachEntry(addToDir)
    byDir
  }

  /**
   * 获取指定目录中的所有日志
   * 
   * @param dir 日志目录
   * @return 目录中的主题分区到日志的映射
   */
  private def logsInDir(dir: File): Map[TopicPartition, UnifiedLog] = {
    logsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  /**
   * 从缓存的日志目录映射中获取指定目录的日志
   * 
   * @param cachedLogsByDir 缓存的日志目录映射
   * @param dir 日志目录
   * @return 目录中的主题分区到日志的映射
   */
  private def logsInDir(cachedLogsByDir: Map[String, Map[TopicPartition, UnifiedLog]],
                        dir: File): Map[TopicPartition, UnifiedLog] = {
    cachedLogsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  /**
   * 检查指定的日志目录是否在线
   * 
   * @param logDir 日志目录的绝对路径
   * @return 如果目录在线返回true，否则返回false
   * @throws LogDirNotFoundException 如果目录不在配置中
   */
  def isLogDirOnline(logDir: String): Boolean = {
    // 检查目录是否在配置中
    if (!logDirs.exists(_.getAbsolutePath == logDir))
      throw new LogDirNotFoundException(s"配置中未找到日志目录 $logDir")

    // 检查目录是否在活跃目录列表中
    _liveLogDirs.contains(new File(logDir))
  }

  /**
   * 刷新那些已超过其刷新间隔且有未写入消息的日志。
   * 此方法会遍历所有当前日志和未来日志，检查每个日志的最后刷新时间，
   * 如果距离上次刷新的时间超过了配置的刷新间隔，则执行刷新操作。
   */
  private def flushDirtyLogs(): Unit = {
    // 记录开始检查脏日志的调试信息
    debug("Checking for dirty logs to flush...")

    // 遍历当前日志和未来日志的合并列表
    for ((topicPartition, log) <- currentLogs.toList ++ futureLogs.toList) {
      try {
        // 计算距离上次刷新的时间间隔
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        // 记录日志刷新检查的详细信息
        debug(s"Checking if flush is needed on ${topicPartition.topic} flush interval ${log.config.flushMs}" +
              s" last flushed ${log.lastFlushTime} time since last flush: $timeSinceLastFlush")
        // 如果超过配置的刷新间隔，执行刷新操作
        if (timeSinceLastFlush >= log.config.flushMs)
          log.flush(false)
      } catch {
        case e: Throwable =>
          // 记录刷新过程中的错误信息
          error(s"Error flushing topic ${topicPartition.topic}", e)
      }
    }
  }

  /**
   * 从日志池中移除指定主题分区的日志，并清理相关的度量指标。
   * 
   * @param logs 日志池，可以是当前日志池或未来日志池
   * @param tp 要移除的主题分区
   * @return 如果成功移除则返回Some(移除的日志)，否则返回None
   */
  private def removeLogAndMetrics(logs: Pool[TopicPartition, UnifiedLog], tp: TopicPartition): Option[UnifiedLog] = {
    // 从日志池中移除指定主题分区的日志
    val removedLog = logs.remove(tp)
    if (removedLog != null) {
      // 如果成功移除，清理相关的度量指标
      removedLog.removeLogMetrics()
      Some(removedLog)
    } else {
      None
    }
  }

  /**
   * 从干净关闭文件中读取Broker纪元。
   * 此方法会检查所有活跃日志目录中的干净关闭文件，验证它们是否包含相同的Broker纪元。
   * 如果任何目录不可用或纪元不一致，则返回空值。
   * 
   * @return 如果所有目录都包含相同的Broker纪元则返回该纪元，否则返回空值
   */
  def readBrokerEpochFromCleanShutdownFiles(): OptionalLong = {
    // 验证所有日志目录是否都处于活跃状态，如果有任何目录不可用，则返回空值
    if (liveLogDirs.size < logDirs.size) {
      return OptionalLong.empty()
    }
    // 用于存储找到的Broker纪元
    var brokerEpoch = -1L
    // 遍历所有活跃的日志目录
    for (dir <- liveLogDirs) {
      // 创建干净关闭文件处理器
      val cleanShutdownFileHandler = new CleanShutdownFileHandler(dir.getPath)
      // 读取当前目录的Broker纪元
      val currentBrokerEpoch = cleanShutdownFileHandler.read
      // 如果无法读取Broker纪元，记录信息并返回空值
      if (!currentBrokerEpoch.isPresent) {
        info(s"Unable to read the broker epoch in ${dir.toString}.")
        return OptionalLong.empty()
      }
      // 如果已经找到了一个Broker纪元，验证当前纪元是否与之相同
      if (brokerEpoch != -1 && currentBrokerEpoch.getAsLong != brokerEpoch) {
        info(s"Found different broker epochs in ${dir.toString}. Other=$brokerEpoch vs current=$currentBrokerEpoch.")
        return OptionalLong.empty()
      }
      brokerEpoch = currentBrokerEpoch.getAsLong
    }
    // 返回找到的一致的Broker纪元
    OptionalLong.of(brokerEpoch)
  }
}

object LogManager {
  val LockFileName = ".lock"

  /**
   * Wait all jobs to complete
   * @param jobs jobs
   * @param callback this will be called to handle the exception caused by each Future#get
   * @return true if all pass. Otherwise, false
   */
  private[log] def waitForAllToComplete(jobs: Seq[Future[_]], callback: Throwable => Unit): Boolean = {
    jobs.count(future => Try(future.get) match {
      case Success(_) => false
      case Failure(e) =>
        callback(e)
        true
    }) == 0
  }

  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"

  def apply(config: KafkaConfig,
            initialOfflineDirs: Seq[String],
            configRepository: ConfigRepository,
            kafkaScheduler: Scheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats,
            logDirFailureChannel: LogDirFailureChannel): LogManager = {
    val defaultProps = config.extractLogConfigMap

    LogConfig.validateBrokerLogConfigValues(defaultProps, config.remoteLogManagerConfig.isRemoteStorageSystemEnabled())
    val defaultLogConfig = new LogConfig(defaultProps)

    val cleanerConfig = LogCleaner.cleanerConfig(config)

    new LogManager(logDirs = config.logDirs.map(new File(_).getAbsoluteFile),
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
      configRepository = configRepository,
      initialDefaultConfig = defaultLogConfig,
      cleanerConfig = cleanerConfig,
      recoveryThreadsPerDataDir = config.numRecoveryThreadsPerDataDir,
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      retentionCheckMs = config.logCleanupIntervalMs,
      maxTransactionTimeoutMs = config.transactionStateManagerConfig.transactionMaxTimeoutMs,
      producerStateManagerConfig = new ProducerStateManagerConfig(config.transactionLogConfig.producerIdExpirationMs, config.transactionLogConfig.transactionPartitionVerificationEnable),
      producerIdExpirationCheckIntervalMs = config.transactionLogConfig.producerIdExpirationCheckIntervalMs,
      scheduler = kafkaScheduler,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      time = time,
      interBrokerProtocolVersion = config.interBrokerProtocolVersion,
      remoteStorageSystemEnable = config.remoteLogManagerConfig.isRemoteStorageSystemEnabled(),
      initialTaskDelayMs = config.logInitialTaskDelayMs)
  }

  /**
   * Returns true if the given log should not be on the current broker
   * according to the metadata image.
   *
   * @param brokerId       The ID of the current broker.
   * @param newTopicsImage The new topics image after broker has been reloaded
   * @param log            The log object to check
   * @return true if the log should not exist on the broker, false otherwise.
   */
  def isStrayKraftReplica(
   brokerId: Int,
   newTopicsImage: TopicsImage,
   log: UnifiedLog
  ): Boolean = {
    if (log.topicId.isEmpty) {
      // Missing topic ID could result from storage failure or unclean shutdown after topic creation but before flushing
      // data to the `partition.metadata` file. And before appending data to the log, the `partition.metadata` is always
      // flushed to disk. So if the topic ID is missing, it mostly means no data was appended, and we can treat this as
      // a stray log.
      info(s"The topicId does not exist in $log, treat it as a stray log")
      return true
    }

    val topicId = log.topicId.get
    val partitionId = log.topicPartition.partition()
    Option(newTopicsImage.getPartition(topicId, partitionId)) match {
      case Some(partition) =>
        if (!partition.replicas.contains(brokerId)) {
          info(s"Found stray log dir $log: the current replica assignment ${partition.replicas.mkString("[", ", ", "]")} " +
            s"does not contain the local brokerId $brokerId.")
          true
        } else {
          false
        }

      case None =>
        info(s"Found stray log dir $log: the topicId $topicId does not exist in the metadata image")
        true
    }
  }
}
