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

package kafka.server.metadata

import org.apache.kafka.image.{MetadataDelta, MetadataImage}

import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.publisher.MetadataPublisher

/**
 * KRaft元数据缓存发布器
 * 负责在KRaft模式下管理和发布Kafka集群的元数据缓存更新
 * 
 * 应用场景：
 * 1. 元数据同步：确保集群中的元数据一致性
 * 2. 缓存更新：实时更新本地元数据缓存
 * 3. 状态管理：维护最新的元数据状态
 */
class KRaftMetadataCachePublisher(
  // KRaft元数据缓存实例，用于存储和管理元数据
  val metadataCache: KRaftMetadataCache
) extends MetadataPublisher {
  
  /**
   * 获取发布器名称
   * @return 发布器的唯一标识名称
   */
  override def name(): String = "KRaftMetadataCachePublisher"

  /**
   * 处理元数据更新
   * 当收到新的元数据变更时，更新本地缓存
   * 
   * @param delta 元数据变更信息，包含了变更的详细内容
   * @param newImage 新的元数据镜像，表示更新后的完整元数据状态
   * @param manifest 加载器清单，包含了元数据加载的相关信息
   */
  override def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
    manifest: LoaderManifest
  ): Unit = {
    // 直接更新元数据缓存的镜像为最新状态
    metadataCache.setImage(newImage)
  }
}

