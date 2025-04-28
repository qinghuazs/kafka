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
package org.apache.kafka.common.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 一个简化迭代器实现的抽象基类
 * 该类通过状态机机制来管理迭代过程，使子类只需要实现具体的元素获取逻辑
 * 应用场景：适用于需要自定义迭代行为的场景，如复杂数据结构的遍历、流式数据处理等
 * @param <T> 迭代器要遍历的元素类型
 */
public abstract class AbstractIterator<T> implements Iterator<T> {

    /**
     * 迭代器的内部状态枚举
     * READY: 下一个元素已准备就绪，可以直接返回
     * NOT_READY: 需要计算下一个元素
     * DONE: 迭代已完成，没有更多元素
     * FAILED: 迭代过程中发生错误
     */
    private enum State {
        READY, NOT_READY, DONE, FAILED
    }

    // 当前迭代器的状态，初始为NOT_READY
    private State state = State.NOT_READY;
    // 下一个要返回的元素
    private T next;

    /**
     * 检查是否还有下一个元素
     * 该方法会根据当前状态决定返回结果或尝试获取下一个元素
     */
    @Override
    public boolean hasNext() {
        switch (state) {
            case FAILED:
                // 如果迭代器处于失败状态，抛出异常
                throw new IllegalStateException("Iterator is in failed state");
            case DONE:
                // 如果迭代已完成，返回false
                return false;
            case READY:
                // 如果已经准备好下一个元素，返回true
                return true;
            default:
                // 尝试计算下一个元素
                return maybeComputeNext();
        }
    }

    /**
     * 获取下一个元素
     * 该方法会先检查是否存在下一个元素，然后返回当前的next值
     */
    @Override
    public T next() {
        // 如果没有下一个元素，抛出异常
        if (!hasNext())
            throw new NoSuchElementException();
        // 将状态设置为NOT_READY，表示需要重新计算下一个元素
        state = State.NOT_READY;
        // 如果next为null，说明实现类的makeNext方法有问题
        if (next == null)
            throw new IllegalStateException("Expected item but none found.");
        return next;
    }

    /**
     * 移除操作未实现
     * 默认抛出UnsupportedOperationException异常
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removal not supported");
    }

    /**
     * 查看下一个元素，但不移动迭代器位置
     * 用于在不消费元素的情况下预览下一个元素
     */
    public T peek() {
        if (!hasNext())
            throw new NoSuchElementException();
        return next;
    }

    /**
     * 标记迭代结束
     * 子类在确定没有更多元素时调用此方法
     */
    protected T allDone() {
        state = State.DONE;
        return null;
    }

    /**
     * 获取下一个元素的抽象方法
     * 子类必须实现此方法以提供具体的元素获取逻辑
     * 当没有更多元素时，应调用allDone()方法
     */
    protected abstract T makeNext();

    /**
     * 尝试计算下一个元素
     * 该方法负责状态转换和调用makeNext获取元素
     */
    private Boolean maybeComputeNext() {
        // 设置状态为FAILED，防止makeNext过程中发生异常导致状态不一致
        state = State.FAILED;
        // 调用子类实现的makeNext方法获取下一个元素
        next = makeNext();
        if (state == State.DONE) {
            // 如果状态为DONE，表示迭代结束
            return false;
        } else {
            // 设置状态为READY，表示已经成功获取到下一个元素
            state = State.READY;
            return true;
        }
    }

}
