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
package org.apache.kafka.common;

/**
 * 用于监控目的的度量指标接口。
 * 该接口定义了Kafka中所有可监控指标的基本结构，包括指标名称和值。
 */
public interface Metric {

    /**
     * 获取该指标的名称
     * 返回一个MetricName对象，包含了指标的名称、分组、描述和标签等信息
     */
    MetricName metricName();

    /**
     * 获取该指标的当前值
     * 返回值可以是一个可测量的数值，也可以是一个不可测量的计量值（gauge）
     * 由于指标值的类型可能多样，所以返回类型为Object
     */
    Object metricValue();

}
