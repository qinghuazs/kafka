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

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * 用于配置Admin#listClientMetricsResources()方法的选项类。
 * 该类用于指定获取客户端指标资源列表时的各种参数和配置选项。
 *
 * 该类的API仍在演进中，详细信息请参见Admin接口的说明。
 */
@InterfaceStability.Evolving
public class ListClientMetricsResourcesOptions extends AbstractOptions<ListClientMetricsResourcesOptions> {
}
