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
package org.apache.kafka.common.config.types;

/**
 * A wrapper class for passwords to hide them while logging a config
 */
/**
 * 密码包装器类
 * 用于在记录配置时隐藏密码内容。
 * 
 * 应用场景：
 * 1. 配置文件中的敏感信息保护
 * 2. 日志记录时的密码脱敏
 * 3. 系统配置的安全展示
 * 4. API接口的密码参数处理
 *
 * 设计考虑：
 * 1. 安全性：toString方法返回固定字符串以隐藏实际密码
 * 2. 不可变性：使用final确保密码值不可修改
 * 3. 封装性：通过value()方法控制对实际密码的访问
 * 4. 比较支持：实现equals和hashCode方法支持对象比较
 */
public class Password {

    /**
     * 隐藏密码的占位符字符串
     * 用于在toString()方法中替代实际密码值
     */
    public static final String HIDDEN = "[hidden]";

    /**
     * 实际的密码值
     * final修饰确保密码值不可变
     */
    private final String value;

    /**
     * 构造函数
     * 创建一个新的Password对象
     *
     * @param value 密码的实际值
     */
    public Password(String value) {
        // 存储密码值
        this.value = value;
    }

    /**
     * 获取密码的实际值
     * 此方法应该谨慎使用，只在必要时调用
     *
     * @return 密码的实际字符串值
     */
    public String value() {
        // 返回实际的密码值
        return value;
    }

    /**
     * 返回隐藏的密码字符串
     * 用于日志记录和对象展示
     *
     * @return 固定的隐藏字符串 "[hidden]"
     */
    @Override
    public String toString() {
        // 返回隐藏字符串而不是实际密码
        return HIDDEN;
    }

    // equals和hashCode方法的实现用于支持对象比较
    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Password))
            return false;
        Password other = (Password) obj;
        return value.equals(other.value);
    }
}
