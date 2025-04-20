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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * MetricName的模板类。包含名称、分组和描述信息，以及用于创建mBean名称的所有标签。
 * 标签值在模板中被省略，但会在运行时填充具体的值。如果提供了有序集合，标签的顺序将被保持，
 * 这样mBean的名称就可以按字典序进行比较和排序。
 */
public class MetricNameTemplate {
    // 指标的名称
    private final String name;
    // 指标所属的分组
    private final String group;
    // 指标的描述信息
    private final String description;
    // 使用LinkedHashSet保持标签的插入顺序
    private final LinkedHashSet<String> tags;

    /**
     * 创建一个新的模板。注意：如果提供的tagsNames集合是有序的，标签的顺序将被保持。
     *
     * @param name 指标的名称，不能为null
     * @param group 指标所属的分组，不能为null
     * @param description 指标的描述信息，不能为null
     * @param tagsNames 指标标签名称的集合，应该是一个保持顺序的集合，不能为null
     */
    public MetricNameTemplate(String name, String group, String description, Set<String> tagsNames) {
        // 使用Objects.requireNonNull确保参数不为null
        this.name = Objects.requireNonNull(name);
        this.group = Objects.requireNonNull(group);
        this.description = Objects.requireNonNull(description);
        this.tags = new LinkedHashSet<>(Objects.requireNonNull(tagsNames));
    }

    /**
     * 创建一个新的模板。这个构造方法接受可变参数形式的标签名称。
     * 注意：标签的顺序将被保持。
     *
     * @param name 指标的名称，不能为null
     * @param group 指标所属的分组，不能为null
     * @param description 指标的描述信息，不能为null
     * @param tagsNames 按照首选顺序排列的指标标签名称，所有标签名称都不能为null
     */
    public MetricNameTemplate(String name, String group, String description, String... tagsNames) {
        // 调用另一个构造方法，将可变参数转换为Set
        this(name, group, description, getTags(tagsNames));
    }

    /**
     * 将可变参数形式的标签名称转换为LinkedHashSet
     * 
     * @param keys 标签名称数组
     * @return 包含所有标签名称的LinkedHashSet
     */
    private static LinkedHashSet<String> getTags(String... keys) {
        LinkedHashSet<String> tags = new LinkedHashSet<>();
        // 将所有标签添加到LinkedHashSet中
        Collections.addAll(tags, keys);
        return tags;
    }

    /**
     * 获取指标的名称
     *
     * @return 指标名称，永不为null
     */
    public String name() {
        return this.name;
    }

    /**
     * 获取指标所属的分组名称
     *
     * @return 分组名称，永不为null
     */
    public String group() {
        return this.group;
    }

    /**
     * 获取指标的描述信息
     *
     * @return 指标描述，永不为null
     */
    public String description() {
        return this.description;
    }

    /**
     * 获取指标的标签名称集合
     *
     * @return 有序的标签名称集合，永不为null但可能为空
     */
    public Set<String> tags() {
        return tags;
    }

    /**
     * 计算对象的哈希码
     * 使用name、group和tags字段计算哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(name, group, tags);
    }

    /**
     * 比较两个MetricNameTemplate对象是否相等
     * 比较name、group和tags字段的值
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MetricNameTemplate other = (MetricNameTemplate) o;
        return Objects.equals(name, other.name) && Objects.equals(group, other.group) &&
                Objects.equals(tags, other.tags);
    }

    /**
     * 返回对象的字符串表示
     * 格式化输出name、group和tags字段的值
     */
    @Override
    public String toString() {
        return String.format("name=%s, group=%s, tags=%s", name, group, tags);
    }
}
