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

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import javax.management.ObjectName;

/**
 * JMX度量指标名称的处理工具类，用于处理名称中的特殊字符。
 * <p>
 * 在所有度量指标名称中，用户主体信息通过 {@link #sanitize(String)} 进行URL编码处理。
 * 在注册JMX时，包括client-id在内的所有其他度量标签如果包含特殊字符，
 * 则使用 {@link #jmxSanitize(String)} 进行引号处理。
 */
public class Sanitizer {

    /**
     * JMX中虽然只有少数字符是不允许使用的，但为了安全起见，这里对包含特殊字符的字符串都进行引号处理。
     * 使用 {@link #sanitize(String)} 处理过的字符串中的所有字符都是JMX安全的，因此这里包含了这些字符。
     * 正则表达式说明：
     * \w: 匹配字母、数字、下划线
     * -: 匹配连字符
     * %: 匹配百分号（URL编码会用到）
     * \.: 匹配点号
     * \s\t: 匹配空格和制表符
     */
    private static final Pattern MBEAN_PATTERN = Pattern.compile("[\\w-%\\. \t]*");

    /**
     * 对输入的名称进行安全处理，使其可以安全用作JMX度量指标名称。
     * 主要处理步骤：
     * 1. 先进行URL编码，处理所有不安全字符
     * 2. 对URL编码后的特殊情况进行额外处理
     */
    public static String sanitize(String name) {
        // 使用UTF-8字符集对名称进行URL编码
        String encoded = URLEncoder.encode(name, StandardCharsets.UTF_8);
        StringBuilder builder = new StringBuilder();
        // 遍历编码后的字符串，处理特殊字符
        for (int i = 0; i < encoded.length(); i++) {
            char c = encoded.charAt(i);
            if (c == '*') {         // 将星号替换为%2A，因为在JMX的ObjectName中星号被视为模式匹配符
                builder.append("%2A");
            } else if (c == '+') {  // URL编码会将空格编码为+号，这里将其替换为%20
                builder.append("%20");
            } else {
                builder.append(c);   // 其他字符保持不变
            }
        }
        return builder.toString();
    }

    /**
     * 将使用 {@link #sanitize(String)} 进行URL编码的名称还原。
     * 直接使用URLDecoder进行解码，使用UTF-8字符集。
     */
    public static String desanitize(String name) {
        return URLDecoder.decode(name, StandardCharsets.UTF_8);
    }

    /**
     * 如果名称中包含不安全的JMX字符，则使用 {@link ObjectName#quote(String)} 进行引号处理。
     * 注意：已经通过 {@link #sanitize(String)} 处理过的用户主体信息不会被加引号，
     * 因为这些字符串已经是JMX安全的了。
     */
    public static String jmxSanitize(String name) {
        // 使用正则表达式判断是否包含特殊字符，如果包含则添加引号，否则保持原样
        return MBEAN_PATTERN.matcher(name).matches() ? name : ObjectName.quote(name);
    }
}
