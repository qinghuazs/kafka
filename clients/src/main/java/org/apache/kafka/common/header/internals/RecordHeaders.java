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
package org.apache.kafka.common.header.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.AbstractIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 记录头部集合类
 * 实现Headers接口，用于管理Kafka记录的头部集合。
 * 支持可变和只读两种模式，提供头部的添加、删除和查询功能。
 * 
 * 应用场景：
 * 1. 消息元数据：管理消息的多个头部信息
 * 2. 消息路由：基于头部信息进行消息分发
 * 3. 消息过滤：根据头部信息过滤消息
 * 4. 数据追踪：记录消息处理的链路信息
 *
 * 设计考虑：
 * 1. 线程安全：使用volatile保证isReadOnly的可见性
 * 2. 可变性控制：支持设置只读状态
 * 3. 迭代器支持：提供安全的迭代器实现
 * 4. 空值处理：不允许null键和头部
 */
public class RecordHeaders implements Headers {

    /**
     * 头部列表
     * 存储所有的Header对象
     */
    private final List<Header> headers;

    /**
     * 只读标志
     * 使用volatile保证多线程可见性
     */
    private volatile boolean isReadOnly;

    /**
     * 默认构造函数
     * 创建空的头部集合
     */
    public RecordHeaders() {
        // 调用另一个构造函数，传入null
        this((Iterable<Header>) null);
    }

    /**
     * 使用头部数组创建头部集合
     *
     * @param headers 头部数组，可以为null
     */
    public RecordHeaders(Header[] headers) {
        // 如果headers为null，传入null，否则转换为List
        this(headers == null ? null : Arrays.asList(headers));
    }

    /**
     * 使用可迭代的头部集合创建头部集合
     * 
     * 实现说明：
     * - 支持高效的复制构造
     * - 处理null和不同类型的输入
     *
     * @param headers 头部集合，可以为null
     */
    public RecordHeaders(Iterable<Header> headers) {
        // 如果输入为null，创建空列表
        if (headers == null) {
            this.headers = new ArrayList<>();
        } 
        // 如果输入是RecordHeaders类型，使用高效的复制构造
        else if (headers instanceof RecordHeaders) {
            this.headers = new ArrayList<>(((RecordHeaders) headers).headers);
        } 
        // 其他情况，遍历添加每个头部
        else {
            this.headers = new ArrayList<>();
            for (Header header : headers) {
                // 验证头部不为null
                Objects.requireNonNull(header, "Header cannot be null.");
                this.headers.add(header);
            }
        }
    }

    /**
     * 添加一个头部
     * 
     * @param header 要添加的头部，不能为null
     * @return this对象，支持链式调用
     * @throws IllegalStateException 如果集合是只读的
     */
    @Override
    public Headers add(Header header) throws IllegalStateException {
        // 验证头部不为null
        Objects.requireNonNull(header, "Header cannot be null.");
        // 检查是否可写
        canWrite();
        // 添加头部
        headers.add(header);
        return this;
    }

    /**
     * 使用键值对添加头部
     *
     * @param key 键，不能为null
     * @param value 值，可以为null
     * @return this对象，支持链式调用
     * @throws IllegalStateException 如果集合是只读的
     */
    @Override
    public Headers add(String key, byte[] value) throws IllegalStateException {
        // 创建新的RecordHeader并添加
        return add(new RecordHeader(key, value));
    }

    /**
     * 移除所有具有指定键的头部
     *
     * @param key 要移除的头部的键
     * @return this对象，支持链式调用
     * @throws IllegalStateException 如果集合是只读的
     */
    @Override
    public Headers remove(String key) throws IllegalStateException {
        // 检查是否可写
        canWrite();
        // 验证键不为null
        checkKey(key);
        // 获取迭代器
        Iterator<Header> iterator = iterator();
        // 遍历并移除匹配的头部
        while (iterator.hasNext()) {
            if (iterator.next().key().equals(key)) {
                iterator.remove();
            }
        }
        return this;
    }

    /**
     * 获取指定键的最后一个头部
     *
     * @param key 要查找的键
     * @return 最后一个匹配的头部，如果没有则返回null
     */
    @Override
    public Header lastHeader(String key) {
        // 验证键不为null
        checkKey(key);
        // 从后向前遍历查找
        for (int i = headers.size() - 1; i >= 0; i--) {
            Header header = headers.get(i);
            if (header.key().equals(key)) {
                return header;
            }
        }
        return null;
    }

    /**
     * 获取所有具有指定键的头部
     *
     * @param key 要查找的键
     * @return 包含所有匹配头部的Iterable对象
     */
    @Override
    public Iterable<Header> headers(final String key) {
        // 验证键不为null
        checkKey(key);
        // 返回过滤后的迭代器
        return () -> new FilterByKeyIterator(headers.iterator(), key);
    }

    /**
     * 获取头部集合的迭代器
     *
     * @return 支持删除操作的迭代器
     */
    @Override
    public Iterator<Header> iterator() {
        // 返回支持关闭检查的迭代器
        return closeAware(headers.iterator());
    }

    /**
     * 设置头部集合为只读
     */
    public void setReadOnly() {
        // 设置只读标志
        this.isReadOnly = true;
    }

    /**
     * 将头部集合转换为数组
     *
     * @return 头部数组
     */
    public Header[] toArray() {
        // 如果集合为空返回空数组，否则转换为数组
        return headers.isEmpty() ? Record.EMPTY_HEADERS : headers.toArray(new Header[0]);     
    }

    /**
     * 验证键不为null
     *
     * @param key 要验证的键
     * @throws IllegalArgumentException 如果键为null
     */
    private void checkKey(String key) {
        // 如果键为null抛出异常
        if (key == null)
            throw new IllegalArgumentException("key cannot be null.");
    }

    /**
     * 检查是否可以写入
     *
     * @throws IllegalStateException 如果集合是只读的
     */
    private void canWrite() {
        // 如果是只读状态抛出异常
        if (isReadOnly)
            throw new IllegalStateException("RecordHeaders has been closed.");
    }

    /**
     * 创建支持关闭检查的迭代器
     *
     * @param original 原始迭代器
     * @return 包装后的迭代器
     */
    private Iterator<Header> closeAware(final Iterator<Header> original) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return original.hasNext();
            }

            public Header next() {
                return original.next();
            }

            @Override
            public void remove() {
                // 检查是否可写
                canWrite();
                // 调用原始迭代器的remove方法
                original.remove();
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RecordHeaders headers1 = (RecordHeaders) o;

        return Objects.equals(headers, headers1.headers);
    }

    @Override
    public int hashCode() {
        return headers != null ? headers.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "RecordHeaders(" +
               "headers = " + headers +
               ", isReadOnly = " + isReadOnly +
               ')';
    }

    /**
     * 按键过滤的迭代器实现
     * 用于过滤出具有指定键的头部
     */
    private static final class FilterByKeyIterator extends AbstractIterator<Header> {

        /**
         * 原始迭代器
         */
        private final Iterator<Header> original;

        /**
         * 要过滤的键
         */
        private final String key;

        /**
         * 创建过滤迭代器
         *
         * @param original 原始迭代器
         * @param key 要过滤的键
         */
        private FilterByKeyIterator(Iterator<Header> original, String key) {
            this.original = original;
            this.key = key;
        }

        /**
         * 获取下一个匹配的头部
         *
         * @return 下一个匹配的头部，如果没有则返回null
         */
        protected Header makeNext() {
            // 循环直到找到匹配的头部或遍历完成
            while (true) {
                if (original.hasNext()) {
                    Header header = original.next();
                    // 如果键不匹配，继续查找
                    if (!header.key().equals(key))
                        continue;
                    // 返回匹配的头部
                    return header;
                }
                // 遍历完成，返回结束标记
                return this.allDone();
            }
        }
    }
}
