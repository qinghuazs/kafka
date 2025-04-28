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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.TransferableChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Kafka通用工具类，提供了一系列静态方法用于处理常见的操作任务。
 * 包括但不限于：
 * - 字符串和字节数组之间的UTF-8编码转换
 * - 集合操作和排序
 * - 主机地址和端口号的解析
 * - 文件和目录操作
 * - 数值格式化和计算
 * 
 * 该类被设计为工具类，因此构造函数是私有的，所有方法都是静态的。
 */
public final class Utils {

    /**
     * 私有构造函数，防止实例化
     */
    private Utils() {}

    /**
     * 用于匹配URI格式的正则表达式模式
     * 支持以下格式：
     * 1. host:port
     * 2. protocol://host:port
     * 3. 支持IPv6地址格式 [ipv6]:port
     */
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("^(?:[0-9a-zA-Z\\-%._]*://)?\\[?([0-9a-zA-Z\\-%._:]*)]?:([0-9]+)");

    /**
     * 用于验证主机名中字符的正则表达式模式
     * 允许的字符包括：数字、字母、连字符、点号和冒号
     */
    private static final Pattern VALID_HOST_CHARACTERS = Pattern.compile("([0-9a-zA-Z\\-%._:]*)");

    /**
     * 用于格式化数字的DecimalFormat实例
     * 最多显示两位小数，用于人类可读的打印输出
     * 使用英语区域设置以确保全球统一的格式
     */
    private static final DecimalFormat TWO_DIGIT_FORMAT = new DecimalFormat("0.##",
        DecimalFormatSymbols.getInstance(Locale.ENGLISH));

    /**
     * 字节大小的单位后缀数组
     * 用于格式化文件大小等字节数据的显示
     * 从字节(B)到尧字节(YB)的单位序列
     */
    private static final String[] BYTE_SCALE_SUFFIXES = new String[] {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};

    /**
     * 系统换行符常量
     * 根据不同操作系统自动选择适当的换行符（Windows: \r\n, Unix: \n）
     */
    public static final String NL = System.lineSeparator();

    /**
     * 类的日志记录器实例
     * 用于记录工具类中的各种操作和异常信息
     */
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    /**
     * 获取集合的有序列表表示
     * 该方法将输入的集合转换为一个不可修改的有序列表
     * 
     * @param collection 要排序的集合
     * @param <T> 集合中对象的类型，必须实现Comparable接口
     * @return 包含集合内容的不可修改的有序列表
     * 
     * 实现说明：
     * 1. 创建输入集合的ArrayList副本
     * 2. 使用Collections.sort()对列表进行排序
     * 3. 返回不可修改的列表视图，防止外部修改
     */
    public static <T extends Comparable<? super T>> List<T> sorted(Collection<T> collection) {
        // 创建集合的可变副本
        List<T> res = new ArrayList<>(collection);
        // 对列表进行排序
        Collections.sort(res);
        // 返回不可修改的列表视图
        return Collections.unmodifiableList(res);
    }

    /**
     * 将UTF8编码的字节数组转换为字符串
     * 这是最基本的UTF8转换方法，用于处理完整的字节数组
     *
     * @param bytes UTF8编码的字节数组
     * @return 解码后的字符串
     * 
     * 实现说明：
     * 使用StandardCharsets.UTF_8确保使用标准的UTF-8字符集进行解码
     */
    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 从ByteBuffer中读取指定长度的UTF8字符串
     * 这个方法不会影响ByteBuffer的位置
     *
     * @param buffer 要读取的ByteBuffer
     * @param length 要读取的字节长度
     * @return 解码后的UTF8字符串
     * 
     * 实现说明：
     * 从当前位置开始读取，偏移量为0
     */
    public static String utf8(ByteBuffer buffer, int length) {
        return utf8(buffer, 0, length);
    }

    /**
     * 从ByteBuffer的当前位置读取到末尾的UTF8字符串
     * 这个方法不会影响ByteBuffer的位置
     *
     * @param buffer 要读取的ByteBuffer
     * @return 解码后的UTF8字符串
     * 
     * 实现说明：
     * 使用buffer.remaining()获取剩余可读取的字节数
     */
    public static String utf8(ByteBuffer buffer) {
        return utf8(buffer, buffer.remaining());
    }

    /**
     * 从ByteBuffer的指定偏移位置读取指定长度的UTF8字符串
     * 这个方法不会影响ByteBuffer的位置
     *
     * @param buffer 要读取的ByteBuffer
     * @param offset 相对于当前位置的偏移量
     * @param length 要读取的字节长度
     * @return 解码后的UTF8字符串
     * 
     * 实现说明：
     * 1. 如果ByteBuffer有底层数组，直接使用数组进行解码
     * 2. 否则，先将数据复制到新的字节数组再解码
     */
    public static String utf8(ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray())
            // 如果ByteBuffer有底层数组，直接使用数组进行解码，提高性能
            return new String(buffer.array(), buffer.arrayOffset() + buffer.position() + offset, length, StandardCharsets.UTF_8);
        else
            // 如果没有底层数组，先复制数据再解码
            return utf8(toArray(buffer, offset, length));
    }

    /**
     * 将字符串转换为UTF-8编码的字节数组
     * 这个方法是字符串到UTF-8字节数组转换的基础方法
     * 
     * @param string 要转换的字符串
     * @return UTF-8编码的字节数组
     * 
     * 应用场景：
     * 1. 网络传输前的字符串编码
     * 2. 文件写入前的字符串转换
     * 3. 消息序列化时的字符串处理
     * 
     * 实现说明：
     * 使用Java标准库的StandardCharsets.UTF_8进行编码，确保跨平台的编码一致性
     */
    public static byte[] utf8(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 获取给定数字的绝对值
     * 这个方法对Integer.MIN_VALUE的处理与Java标准库不同
     * 当输入为Integer.MIN_VALUE时返回0，而不是保持Integer.MIN_VALUE
     * 
     * @param n 要计算绝对值的整数
     * @return 整数的绝对值，特别地，当n为Integer.MIN_VALUE时返回0
     * 
     * 应用场景：
     * 1. 需要安全处理Integer.MIN_VALUE的场景
     * 2. 对数值取绝对值且不希望出现负数的场景
     * 
     * 实现说明：
     * 1. 使用三元运算符检查是否为Integer.MIN_VALUE
     * 2. 是则返回0，否则使用Math.abs计算绝对值
     */
    public static int abs(int n) {
        return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
    }

    /**
     * 获取一组long类型值中的最小值
     * 该方法支持可变参数，允许比较任意数量的long值
     * 
     * @param first 第一个值，确保至少有一个参数
     * @param rest 其余要比较的值，可以是0个或多个
     * @return 所有输入值中的最小值
     * 
     * 应用场景：
     * 1. 比较多个时间戳获取最早时间
     * 2. 比较多个偏移量获取最小偏移
     * 3. 在需要处理可变数量参数的场景下获取最小值
     * 
     * 实现说明：
     * 1. 将第一个参数作为初始最小值
     * 2. 遍历rest数组中的每个值
     * 3. 如果发现更小的值则更新最小值
     */
    public static long min(long first, long... rest) {
        long min = first;
        for (long r : rest) {
            if (r < min)
                min = r;
        }
        return min;
    }

    /**
     * 获取一组long类型值中的最大值
     * 该方法支持可变参数，允许比较任意数量的long值
     * 
     * @param first 第一个值，确保至少有一个参数
     * @param rest 其余要比较的值，可以是0个或多个
     * @return 所有输入值中的最大值
     * 
     * 应用场景：
     * 1. 比较多个时间戳获取最晚时间
     * 2. 比较多个偏移量获取最大偏移
     * 3. 在需要处理可变数量参数的场景下获取最大值
     * 
     * 实现说明：
     * 1. 将第一个参数作为初始最大值
     * 2. 遍历rest数组中的每个值
     * 3. 如果发现更大的值则更新最大值
     */
    public static long max(long first, long... rest) {
        long max = first;
        for (long r : rest) {
            if (r > max)
                max = r;
        }
        return max;
    }

    /**
     * 获取两个short类型值中的最小值
     * 
     * @param first 第一个short值
     * @param second 第二个short值
     * @return 两个值中的最小值
     * 
     * 应用场景：
     * 1. 比较两个short类型的配置值
     * 2. 在需要类型安全的short值比较场景
     * 
     * 实现说明：
     * 使用Math.min进行比较，并将结果强制转换回short类型
     */
    public static short min(short first, short second) {
        return (short) Math.min(first, second);
    }

    /**
     * 计算字符串UTF-8编码后的字节长度，无需实际进行编码转换
     * 这个方法通过分析字符串中的每个字符来计算其UTF-8编码后的长度
     *
     * @param s 要计算长度的字符串
     * @return UTF-8编码后的字节长度
     * 
     * 应用场景：
     * 1. 预分配缓冲区大小
     * 2. 验证消息大小限制
     * 3. 网络传输前的容量规划
     * 
     * 实现说明：
     * 1. 遍历字符串中的每个字符
     * 2. 根据Unicode编码范围确定UTF-8编码后的字节数：
     *    - ASCII字符(0-127)：使用1个字节
     *    - 双字节字符(128-2047)：使用2个字节
     *    - 代理对字符：使用4个字节并跳过低代理
     *    - 其他字符：使用3个字节
     */
    public static int utf8Length(CharSequence s) {
        int count = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char ch = s.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }

    /**
     * 将ByteBuffer从当前位置到限制位置的内容读取到字节数组中
     * 
     * @param buffer 要读取的ByteBuffer
     * @return 包含缓冲区内容的字节数组
     * 
     * 应用场景：
     * 1. 网络数据包的内容提取
     * 2. 文件内容的缓冲区读取
     * 
     * 实现说明：
     * 调用toArray(buffer, 0, buffer.remaining())完成实际的读取操作
     */
    public static byte[] toArray(ByteBuffer buffer) {
        return toArray(buffer, 0, buffer.remaining());
    }

    /**
     * 从ByteBuffer的当前位置读取指定大小的字节数组
     * 
     * @param buffer 要读取的ByteBuffer
     * @param size 要读取的字节数
     * @return 包含读取内容的字节数组
     * 
     * 应用场景：
     * 1. 读取固定长度的消息内容
     * 2. 处理带长度前缀的数据包
     * 
     * 实现说明：
     * 调用toArray(buffer, 0, size)完成实际的读取操作
     */
    public static byte[] toArray(ByteBuffer buffer, int size) {
        return toArray(buffer, 0, size);
    }

    /**
     * 将ByteBuffer转换为可空的字节数组
     * 
     * @param buffer 要转换的ByteBuffer，可以为null
     * @return 如果输入为null则返回null，否则返回包含缓冲区内容的字节数组
     * 
     * 应用场景：
     * 1. 处理可能为空的缓冲区数据
     * 2. 在需要null安全转换的场景
     * 
     * 实现说明：
     * 使用三元运算符进行null检查，非null时调用toArray()方法
     */
    public static byte[] toNullableArray(ByteBuffer buffer) {
        return buffer == null ? null : toArray(buffer);
    }

    /**
     * 将字节数组包装为可为空的ByteBuffer对象。
     * 这个方法提供了一个安全的方式来处理可能为空的字节数组，并将其转换为ByteBuffer。
     * 
     * @param array 要包装的字节数组，可以为null
     * @return 如果输入数组为null则返回null，否则返回包装该数组的ByteBuffer对象
     * 
     * 实现说明：
     * 1. 首先检查输入数组是否为null
     * 2. 如果数组为null，直接返回null
     * 3. 如果数组不为null，使用ByteBuffer.wrap()方法创建一个新的ByteBuffer
     */
    public static ByteBuffer wrapNullable(byte[] array) {
        // 使用三元运算符进行null检查，如果数组为null则返回null，否则将数组包装为ByteBuffer
        return array == null ? null : ByteBuffer.wrap(array);
    }

    /**
     * 从ByteBuffer中读取指定偏移量和大小的字节数组
     * 这个方法提供了一种高效的方式来从ByteBuffer中提取数据，同时保持原始缓冲区的位置不变
     * 
     * @param buffer 要读取的ByteBuffer
     * @param offset 相对于当前位置的偏移量
     * @param size 要读取的字节数
     * @return 包含读取数据的字节数组
     * 
     * 应用场景：
     * 1. 网络通信中读取消息体
     * 2. 文件读取时提取特定区域的数据
     * 3. 序列化/反序列化操作
     * 
     * 实现说明：
     * 1. 创建指定大小的目标字节数组
     * 2. 根据ByteBuffer的实现类型选择最优的复制方式：
     *    - 如果有底层数组，直接使用System.arraycopy进行内存复制
     *    - 否则，通过临时改变位置进行读取
     * 3. 保持原始ByteBuffer的位置不变
     */
    public static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        // 创建目标数组
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            // 如果ByteBuffer有底层数组，使用System.arraycopy直接复制，这是最高效的方式
            System.arraycopy(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            // 如果是直接缓冲区，需要通过get方法读取
            int pos = buffer.position();
            // 移动到指定位置
            buffer.position(pos + offset);
            // 读取数据
            buffer.get(dest);
            // 恢复原始位置
            buffer.position(pos);
        }
        return dest;
    }

    /**
     * 从ByteBuffer的当前位置读取一个带大小前缀的字节数组
     * 该方法首先读取一个整数作为数组大小，然后读取相应大小的字节数组
     * 
     * @param buffer 要读取的ByteBuffer
     * @return 读取的字节数组
     * 
     * 应用场景：
     * 1. 读取变长消息，其中消息长度作为前缀
     * 2. 处理网络协议中的动态长度字段
     * 3. 反序列化可变长度数据结构
     * 
     * 实现说明：
     * 1. 读取4字节整数作为数组大小
     * 2. 调用getNullableArray读取实际数据
     * 注意：这个方法会改变buffer的位置
     */
    public static byte[] getNullableSizePrefixedArray(final ByteBuffer buffer) {
        // 读取数组大小（4字节整数）
        final int size = buffer.getInt();
        // 读取指定大小的数组
        return getNullableArray(buffer, size);
    }

    /**
     * 从ByteBuffer中读取指定大小的字节数组，支持null值
     * 这个方法会消费缓冲区：返回时，缓冲区的位置会移动到读取的数组之后
     * 
     * @param buffer 要读取的ByteBuffer
     * @param size 要读取的字节数，-1表示返回null
     * @return 读取的字节数组，如果size为-1则返回null
     * 
     * 应用场景：
     * 1. 读取可能为null的字段
     * 2. 处理带有特殊标记的可选数据
     * 3. 反序列化支持null值的数据结构
     * 
     * 实现说明：
     * 1. 首先检查剩余空间是否足够
     * 2. 处理特殊的size值（-1表示null）
     * 3. 读取数据到新分配的数组
     */
    public static byte[] getNullableArray(final ByteBuffer buffer, final int size) {
        // 检查是否有足够的剩余空间
        if (size > buffer.remaining()) {
            // 提前抛出异常，避免不必要的数组分配
            throw new BufferUnderflowException();
        }
        // 处理size为-1的特殊情况，表示null值
        final byte[] oldBytes = size == -1 ? null : new byte[size];
        if (oldBytes != null) {
            // 读取数据到数组
            buffer.get(oldBytes);
        }
        return oldBytes;
    }

    /**
     * 创建源字节数组的副本
     * 
     * @param src 要复制的源字节数组
     * @return 源数组的完整副本
     * 
     * 应用场景：
     * 1. 需要对数据进行安全隔离时
     * 2. 防止外部修改影响内部数据
     * 3. 创建数据快照
     * 
     * 实现说明：
     * 使用Arrays.copyOf进行数组复制，这会创建一个新的数组并复制所有元素
     */
    public static byte[] copyArray(byte[] src) {
        return Arrays.copyOf(src, src.length);
    }

    /**
     * 使用常量时间算法比较两个字符数组是否相等
     * 这种比较方式主要用于密码等敏感数据的比较，可以防止时序攻击
     * 
     * @param first 第一个要比较的字符数组
     * @param second 第二个要比较的字符数组
     * @return 如果数组相等返回true，否则返回false
     * 
     * 应用场景：
     * 1. 密码验证
     * 2. 安全令牌比较
     * 3. 需要防止时序攻击的场景
     * 
     * 实现说明：
     * 1. 首先进行快速路径检查（相同引用、null值）
     * 2. 检查空数组的特殊情况
     * 3. 使用常量时间算法进行比较：
     *    - 总是遍历第一个数组的所有字符
     *    - 比较时间只依赖于第一个数组的长度
     *    - 即使发现不匹配也继续比较
     * 
     * 安全特性：
     * - 防止时序攻击：执行时间不依赖于数据内容
     * - 避免信息泄露：不会提前返回比较结果
     */
    public static boolean isEqualConstantTime(char[] first, char[] second) {
        // 快速路径：检查是否是同一个数组引用
        if (first == second) {
            return true;
        }
        // 空值检查
        if (first == null || second == null) {
            return false;
        }

        // 处理空数组的特殊情况
        if (second.length == 0) {
            return first.length == 0;
        }

        // 常量时间比较：始终比较first数组的所有字符
        boolean matches = first.length == second.length;
        for (int i = 0; i < first.length; ++i) {
            // 如果second数组较短，重复使用其第一个元素
            int j = i < second.length ? i : 0;
            // 即使已经发现不匹配，也继续比较所有字符
            if (first[i] != second[j]) {
                matches = false;
            }
        }
        return matches;
    }

    /**
     * 使系统线程休眠指定的毫秒数
     * 
     * @param ms 休眠的毫秒数
     */
    public static void sleep(long ms) {
        try {
            // 调用Thread.sleep()使当前线程休眠
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // 如果线程被中断，重新设置中断状态
            // 这里不抛出异常是因为提前唤醒是可以接受的
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 通过反射机制实例化一个类
     * 该方法要求类必须有一个公共的无参构造函数
     * 
     * @param <T> 要实例化的类的类型
     * @param c 要实例化的类的Class对象
     * @return 类的新实例
     * @throws KafkaException 如果实例化过程中发生错误
     */
    public static <T> T newInstance(Class<T> c) {
        // 检查传入的Class对象是否为null
        if (c == null)
            throw new KafkaException("class cannot be null");
        try {
            // 使用反射获取无参构造函数并创建实例
            return c.getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException e) {
            // 如果找不到无参构造函数，抛出异常
            throw new KafkaException("Could not find a public no-argument constructor for " + c.getName(), e);
        } catch (ReflectiveOperationException | RuntimeException e) {
            // 处理其他反射相关异常或运行时异常
            throw new KafkaException("Could not instantiate class " + c.getName(), e);
        }
    }

    /**
     * 根据类名查找并实例化一个类，该类必须是指定基类的子类
     * 
     * @param klass 要实例化的类的全限定名
     * @param base 要实例化的类必须继承的基类
     * @param <T> 基类的类型
     * @return 类的新实例
     * @throws ClassNotFoundException 如果找不到指定的类
     */
    public static <T> T newInstance(String klass, Class<T> base) throws ClassNotFoundException {
        // 先加载类，然后创建实例
        return Utils.newInstance(loadClass(klass, base));
    }

    /**
     * 根据类名加载一个类，该类必须是指定基类的子类
     * 
     * @param klass 要加载的类的全限定名
     * @param base 要加载的类必须继承的基类
     * @param <T> 基类的类型
     * @return 加载的类的Class对象
     * @throws ClassNotFoundException 如果找不到指定的类
     */
    public static <T> Class<? extends T> loadClass(String klass, Class<T> base) throws ClassNotFoundException {
        // 获取上下文类加载器或Kafka的类加载器
        ClassLoader contextOrKafkaClassLoader = Utils.getContextOrKafkaClassLoader();
        
        // 使用loadClass而不是Class.forName，因为类名可能是别名
        // 如果使用Class.forName，当类名是别名时可能会抛出异常
        Class<?> loadedClass = contextOrKafkaClassLoader.loadClass(klass);
        
        // 使用真实的类名调用forName来确保类的初始化
        // 同时验证加载的类是否是base的子类
        return Class.forName(loadedClass.getName(), true, contextOrKafkaClassLoader).asSubclass(base);
    }

    /**
     * 将一个类转换为指定的基类类型并实例化它
     * 
     * @param klass 要实例化的类的Class对象
     * @param base 基类的Class对象
     * @param <T> 基类的类型
     * @return 类的新实例
     * @throws ClassCastException 如果klass不是base的子类
     */
    public static <T> T newInstance(Class<?> klass, Class<T> base) {
        // 将klass转换为base的子类类型，然后创建实例
        return Utils.newInstance(klass.asSubclass(base));
    }

    /**
     * 使用类名和参数构造一个新对象
     * 参数以类型-值对的形式提供，例如：(String.class, "name", Integer.class, 42)
     *
     * @param className 要构造的类的全限定名
     * @param params 构造函数参数，格式为：(参数1类型, 参数1值, 参数2类型, 参数2值, ...)
     * @param <T> 要构造的对象的类型
     * @return 构造的新对象
     * @throws ClassNotFoundException 如果构造对象时发生问题
     */
    public static <T> T newParameterizedInstance(String className, Object... params)
            throws ClassNotFoundException {
        // 创建参数类型数组和参数值数组
        Class<?>[] argTypes = new Class<?>[params.length / 2];
        Object[] args = new Object[params.length / 2];
        try {
            // 加载类
            Class<?> c = Utils.loadClass(className, Object.class);
            // 解析参数类型和值
            for (int i = 0; i < params.length / 2; i++) {
                argTypes[i] = (Class<?>) params[2 * i];
                args[i] = params[(2 * i) + 1];
            }
            // 获取构造函数并创建实例
            @SuppressWarnings("unchecked")
            Constructor<T> constructor = (Constructor<T>) c.getConstructor(argTypes);
            return constructor.newInstance(args);
        } catch (NoSuchMethodException e) {
            // 找不到匹配的构造函数
            throw new ClassNotFoundException(String.format("Failed to find " +
                "constructor with %s for %s", Arrays.stream(argTypes).map(Object::toString).collect(Collectors.joining(", ")), className), e);
        } catch (InstantiationException e) {
            // 实例化失败
            throw new ClassNotFoundException(String.format("Failed to instantiate " +
                "%s", className), e);
        } catch (IllegalAccessException e) {
            // 无法访问构造函数
            throw new ClassNotFoundException(String.format("Unable to access " +
                "constructor of %s", className), e);
        } catch (InvocationTargetException e) {
            // 构造函数抛出异常
            throw new KafkaException(String.format("The constructor of %s threw an exception", className), e.getCause());
        }
    }

    /**
     * 使用MurmurHash2算法生成32位哈希值
     * MurmurHash是一种非加密型哈希函数，适用于一般的哈希检索操作
     * 
     * @param data 要计算哈希值的字节数组
     * @return 32位哈希值
     */
    @SuppressWarnings("fallthrough")
    public static int murmur2(final byte[] data) {
        // 获取数据长度
        int length = data.length;
        // 初始化种子值
        int seed = 0x9747b28c;
        // 'm'和'r'是预先计算好的混合常量
        final int m = 0x5bd1e995;
        final int r = 24;

        // 使用seed和数据长度初始化哈希值
        int h = seed ^ length;
        // 计算4字节为一组的组数
        int length4 = length / 4;

        // 主循环：每次处理4个字节
        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            // 将4个字节组合成一个32位整数
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            // 对k进行混合
            k *= m;
            k ^= k >>> r;
            k *= m;
            // 更新哈希值
            h *= m;
            h ^= k;
        }

        // 处理剩余的字节（0-3个字节）
        switch (length % 4) {
            case 3: // 处理剩余的3个字节
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2: // 处理剩余的2个字节
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1: // 处理剩余的1个字节
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        // 最终的混合
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    /**
     * 从地址字符串中提取主机名
     * 
     * @param address 要解析的地址字符串，格式为"host:port"或"protocol://host:port"或"[ipv6]:port"
     * @return 提取的主机名，如果地址格式不正确则返回null
     * 
     * 应用场景：
     * 1. 解析网络连接地址
     * 2. 配置文件中的地址解析
     * 3. 服务发现和注册
     * 
     * 实现说明：
     * 1. 使用预定义的HOST_PORT_PATTERN正则表达式匹配地址
     * 2. 如果匹配成功，返回第一个捕获组（主机名部分）
     * 3. 如果匹配失败，返回null
     */
    public static String getHost(String address) {
        // 使用正则表达式匹配地址字符串
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        // 如果匹配成功，返回主机名部分（第一个捕获组），否则返回null
        return matcher.matches() ? matcher.group(1) : null;
    }

    /**
     * 从地址字符串中提取端口号
     * 
     * @param address 要解析的地址字符串，格式为"host:port"或"protocol://host:port"或"[ipv6]:port"
     * @return 提取的端口号，如果地址格式不正确则返回null
     * 
     * 应用场景：
     * 1. 网络服务配置解析
     * 2. 端口可用性检查
     * 3. 服务监听端口获取
     * 
     * 实现说明：
     * 1. 使用预定义的HOST_PORT_PATTERN正则表达式匹配地址
     * 2. 如果匹配成功，将第二个捕获组（端口号部分）转换为整数
     * 3. 如果匹配失败，返回null
     */
    public static Integer getPort(String address) {
        // 使用正则表达式匹配地址字符串
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        // 如果匹配成功，将端口号部分（第二个捕获组）转换为整数，否则返回null
        return matcher.matches() ? Integer.parseInt(matcher.group(2)) : null;
    }

    /**
     * 验证主机名是否只包含有效字符
     * 
     * @param address 要验证的主机名字符串
     * @return 如果主机名只包含有效字符则返回true
     * 
     * 应用场景：
     * 1. 主机名格式验证
     * 2. 配置文件中的主机名检查
     * 3. 用户输入验证
     * 
     * 实现说明：
     * 使用预定义的VALID_HOST_CHARACTERS正则表达式验证主机名是否只包含允许的字符
     * 允许的字符包括：数字、字母、连字符、点号和冒号
     */
    public static boolean validHostPattern(String address) {
        // 使用正则表达式验证主机名是否只包含有效字符
        return VALID_HOST_CHARACTERS.matcher(address).matches();
    }

    /**
     * 将主机名和端口号格式化为地址字符串
     * 
     * @param host 主机名
     * @param port 端口号
     * @return 格式化后的地址字符串
     * 
     * 应用场景：
     * 1. 网络地址显示
     * 2. 日志记录
     * 3. 配置文件生成
     * 
     * 实现说明：
     * 1. 检查主机名是否包含冒号（用于判断是否为IPv6地址）
     * 2. 如果是IPv6地址，使用方括号包围主机名
     * 3. 最后添加冒号和端口号
     */
    public static String formatAddress(String host, Integer port) {
        // 如果主机名包含冒号（IPv6地址），使用方括号包围
        return host.contains(":")
                ? "[" + host + "]:" + port // IPv6格式
                : host + ":" + port;       // IPv4或主机名格式
    }

    /**
     * 将字节大小格式化为人类可读的字符串
     * 
     * @param bytes 要格式化的字节大小
     * @return 格式化后的字符串，包含适当的单位（如"3.2 KB"、"1.5 MB"、"2.1 GB"等）
     *         对于负值或超大值，直接返回其字符串表示
     * 
     * 应用场景：
     * 1. 文件大小显示
     * 2. 内存使用量报告
     * 3. 网络带宽显示
     * 
     * 实现说明：
     * 1. 处理特殊情况：负值直接返回
     * 2. 计算适当的单位级别（B、KB、MB等）
     * 3. 进行单位转换并格式化数值（保留两位小数）
     * 4. 添加单位后缀
     */
    public static String formatBytes(long bytes) {
        // 处理负值
        if (bytes < 0) {
            return String.valueOf(bytes);
        }
        // 转换为double以进行浮点运算
        double asDouble = (double) bytes;
        // 计算合适的单位级别（0表示B，1表示KB，2表示MB，以此类推）
        int ordinal = (int) Math.floor(Math.log(asDouble) / Math.log(1024.0));
        // 计算转换后的比例
        double scale = Math.pow(1024.0, ordinal);
        // 转换到目标单位
        double scaled = asDouble / scale;
        // 格式化数值，保留两位小数
        String formatted = TWO_DIGIT_FORMAT.format(scaled);
        try {
            // 添加单位后缀
            return formatted + " " + BYTE_SCALE_SUFFIXES[ordinal];
        } catch (IndexOutOfBoundsException e) {
            // 处理超大数值（超出单位数组范围）
            return String.valueOf(asDouble);
        }
    }

    /**
     * 将Map转换为格式化的字符串
     * 
     * @param map Map对象
     * @param begin 起始字符串
     * @param end 结束字符串
     * @param keyValueSeparator 键值对之间的分隔符
     * @param elementSeparator 元素之间的分隔符
     * @return 格式化后的字符串
     * 
     * 应用场景：
     * 1. 配置信息的字符串表示
     * 2. 调试信息输出
     * 3. 日志记录
     * 
     * 示例：
     * mkString({key: "hello", keyTwo: "hi"}, "|START|", "|END|", "=", ",")
     * 返回："|START|key=hello,keyTwo=hi|END|"
     * 
     * 实现说明：
     * 1. 创建StringBuilder并添加起始字符串
     * 2. 遍历Map中的每个条目，添加键值对
     * 3. 使用分隔符连接多个条目
     * 4. 最后添加结束字符串
     */
    public static <K, V> String mkString(Map<K, V> map, String begin, String end,
                                         String keyValueSeparator, String elementSeparator) {
        // 创建StringBuilder用于构建结果字符串
        StringBuilder bld = new StringBuilder();
        // 添加起始字符串
        bld.append(begin);
        // 用于第一个元素之前不添加分隔符
        String prefix = "";
        // 遍历Map中的所有条目
        for (Map.Entry<K, V> entry : map.entrySet()) {
            // 添加分隔符（第一个元素除外）、键、分隔符和值
            bld.append(prefix).append(entry.getKey()).
                    append(keyValueSeparator).append(entry.getValue());
            // 更新前缀为元素分隔符
            prefix = elementSeparator;
        }
        // 添加结束字符串
        bld.append(end);
        return bld.toString();
    }

    /**
     * 将格式化的字符串解析为Map
     * 
     * @param mapStr 要解析的字符串
     * @param keyValueSeparator 键值对之间的分隔符
     * @param elementSeparator 元素之间的分隔符
     * @return 解析后的Map对象
     * 
     * 应用场景：
     * 1. 配置字符串解析
     * 2. 命令行参数解析
     * 3. URL查询参数解析
     * 
     * 示例：
     * parseMap("key=hey,keyTwo=hi,keyThree=hello", "=", ",")
     * 返回：{key: "hey", keyTwo: "hi", keyThree: "hello"}
     * 
     * 实现说明：
     * 1. 创建空的HashMap
     * 2. 如果输入字符串不为空，按元素分隔符拆分
     * 3. 对每个部分按键值分隔符拆分
     * 4. 将键值对添加到Map中
     */
    public static Map<String, String> parseMap(String mapStr, String keyValueSeparator, String elementSeparator) {
        // 创建用于存储结果的Map
        Map<String, String> map = new HashMap<>();

        // 只处理非空字符串
        if (!mapStr.isEmpty()) {
            // 按元素分隔符拆分字符串
            String[] attrvals = mapStr.split(elementSeparator);
            // 处理每个键值对
            for (String attrval : attrvals) {
                // 按键值分隔符拆分，限制拆分次数为2（避免值中包含分隔符时的问题）
                String[] array = attrval.split(keyValueSeparator, 2);
                // 将键值对添加到Map中
                map.put(array[0], array[1]);
            }
        }
        return map;
    }

    /**
     * 从指定路径读取属性文件
     * 
     * @param filename 要读取的文件路径
     * @return 加载的属性对象
     * @throws IOException 如果文件读取过程中发生IO错误
     * 
     * 应用场景：
     * 1. 加载配置文件
     * 2. 读取系统属性
     * 3. 应用程序初始化
     * 
     * 实现说明：
     * 调用重载的loadProps方法，不指定键过滤
     */
    public static Properties loadProps(String filename) throws IOException {
        return loadProps(filename, null);
    }

    /**
     * 从指定路径读取属性文件，可以指定只加载特定的键
     * 
     * @param filename 要读取的文件路径
     * @param onlyIncludeKeys 指定要包含的键列表，如果为null则加载所有键
     * @return 加载的属性对象
     * @throws IOException 如果文件读取过程中发生IO错误
     * 
     * 应用场景：
     * 1. 选择性加载配置
     * 2. 配置文件部分读取
     * 3. 配置迁移和过滤
     * 
     * 实现说明：
     * 1. 创建Properties对象
     * 2. 如果文件名不为null，打开文件并加载属性
     * 3. 如果指定了键列表，创建新的Properties只包含指定的键
     */
    public static Properties loadProps(String filename, List<String> onlyIncludeKeys) throws IOException {
        // 创建Properties对象
        Properties props = new Properties();

        // 如果文件名不为null，加载文件内容
        if (filename != null) {
            try (InputStream propStream = Files.newInputStream(Paths.get(filename))) {
                props.load(propStream);
            }
        } else {
            System.out.println("Did not load any properties since the property file is not specified");
        }

        // 如果没有指定键列表或列表为空，返回所有属性
        if (onlyIncludeKeys == null || onlyIncludeKeys.isEmpty())
            return props;
        
        // 创建新的Properties对象，只包含指定的键
        Properties requestedProps = new Properties();
        onlyIncludeKeys.forEach(key -> {
            String value = props.getProperty(key);
            if (value != null)
                requestedProps.setProperty(key, value);
        });
        return requestedProps;
    }

    /**
     * 将Properties对象转换为Map<String, String>
     * 
     * @param props 要转换的Properties对象
     * @return 转换后的Map对象
     * 
     * 应用场景：
     * 1. 属性对象的Map表示
     * 2. 配置数据的格式转换
     * 3. 序列化准备
     * 
     * 实现说明：
     * 1. 创建HashMap存储结果
     * 2. 遍历Properties中的所有条目
     * 3. 将键和值都转换为字符串并存入Map
     */
    public static Map<String, String> propsToStringMap(Properties props) {
        // 创建结果Map
        Map<String, String> result = new HashMap<>();
        // 遍历Properties中的所有条目，转换为字符串键值对
        for (Map.Entry<Object, Object> entry : props.entrySet())
            result.put(entry.getKey().toString(), entry.getValue().toString());
        return result;
    }

    /**
     * 获取异常的堆栈跟踪字符串
     * 
     * @param e 要获取堆栈跟踪的异常对象
     * @return 格式化的堆栈跟踪字符串
     * 
     * 应用场景：
     * 1. 异常日志记录
     * 2. 调试信息收集
     * 3. 错误报告生成
     * 
     * 实现说明：
     * 1. 创建StringWriter和PrintWriter
     * 2. 将异常的堆栈跟踪打印到StringWriter
     * 3. 转换为字符串返回
     */
    public static String stackTrace(Throwable e) {
        // 创建字符串写入器
        StringWriter sw = new StringWriter();
        // 创建打印写入器
        PrintWriter pw = new PrintWriter(sw);
        // 将异常堆栈打印到写入器
        e.printStackTrace(pw);
        // 返回堆栈跟踪字符串
        return sw.toString();
    }

    /**
     * 从ByteBuffer中读取指定偏移量和长度的字节数组
     * 
     * @param buffer 源ByteBuffer
     * @param offset 起始偏移量
     * @param length 要读取的长度
     * @return 包含读取数据的字节数组
     * 
     * 应用场景：
     * 1. 网络数据包处理
     * 2. 文件数据读取
     * 3. 消息解析
     * 
     * 实现说明：
     * 1. 创建目标字节数组
     * 2. 根据ByteBuffer是否有底层数组选择不同的复制方式：
     *    - 有底层数组时使用System.arraycopy
     *    - 没有底层数组时使用get方法
     * 3. 保持原始ByteBuffer的位置不变
     */
    public static byte[] readBytes(ByteBuffer buffer, int offset, int length) {
        // 创建目标字节数组
        byte[] dest = new byte[length];
        if (buffer.hasArray()) {
            // 如果ByteBuffer有底层数组，直接使用System.arraycopy进行内存复制
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, length);
        } else {
            // 如果是直接缓冲区，需要通过get方法读取
            // 标记当前位置
            buffer.mark();
            // 移动到指定偏移位置
            buffer.position(offset);
            // 读取数据到目标数组
            buffer.get(dest);
            // 恢复原始位置
            buffer.reset();
        }
        return dest;
    }

    /**
     * 将ByteBuffer转换为字节数组
     * 这个方法会读取缓冲区从当前位置到限制位置的所有内容
     * 
     * @param buffer 要读取的ByteBuffer
     * @return 包含缓冲区内容的字节数组
     * 
     * 应用场景：
     * 1. 网络通信中读取消息内容
     * 2. 文件读取操作
     * 3. 序列化数据处理
     * 
     * 实现说明：
     * 调用重载方法readBytes，从位置0读取到缓冲区的限制位置
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        return Utils.readBytes(buffer, 0, buffer.limit());
    }

    /**
     * 从源缓冲区读取指定字节数并返回新的缓冲区
     * 新缓冲区将从源缓冲区的当前位置开始，两个缓冲区共享内容但位置、限制和标记是独立的
     * 
     * @param srcBuf 源缓冲区
     * @param bytesToRead 要读取的字节数
     * @return 目标缓冲区，如果bytesToRead小于0则返回null
     * 
     * 应用场景：
     * 1. 需要共享缓冲区内容但独立操作的场景
     * 2. 读取固定长度的消息或数据块
     * 3. 流式处理大数据时的分块操作
     * 
     * 实现说明：
     * 1. 首先检查读取长度的有效性
     * 2. 使用slice()创建共享内容的新缓冲区
     * 3. 设置新缓冲区的限制和源缓冲区的位置
     * 
     * 注意：从JDK 13开始，可以使用slice(int index, int length)替代此方法
     */
    public static ByteBuffer readBytes(ByteBuffer srcBuf, int bytesToRead) {
        // 检查读取长度的有效性
        if (bytesToRead < 0)
            return null;

        // 创建共享内容的新缓冲区
        final ByteBuffer dstBuf = srcBuf.slice();
        // 设置新缓冲区的限制
        dstBuf.limit(bytesToRead);
        // 更新源缓冲区的位置
        srcBuf.position(srcBuf.position() + bytesToRead);

        return dstBuf;
    }

    /**
     * 将文件内容读取为字符串
     * 文件被视为流进行处理，不执行随机访问操作
     * 
     * @param path 文件路径
     * @return 文件内容的字符串表示
     * @throws IOException 当文件读取失败时抛出
     * 
     * 应用场景：
     * 1. 配置文件的读取
     * 2. 日志文件的处理
     * 3. 文本文件的加载
     * 
     * 实现说明：
     * 1. 使用Files.readAllBytes读取所有字节
     * 2. 使用UTF-8编码将字节转换为字符串
     * 3. 包装IO异常提供更多上下文信息
     */
    public static String readFileAsString(String path) throws IOException {
        try {
            // 读取文件的所有字节
            byte[] allBytes = Files.readAllBytes(Paths.get(path));
            // 使用UTF-8编码转换为字符串
            return new String(allBytes, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            // 包装异常，提供更多上下文信息
            throw new IOException("Unable to read file " + path, ex);
        }
    }

    /**
     * 确保ByteBuffer具有足够的容量
     * 如果现有缓冲区容量不足，创建新的更大容量的缓冲区
     * 
     * @param existingBuffer 现有的ByteBuffer
     * @param newLength 所需的新长度
     * @return 具有足够容量的ByteBuffer
     * 
     * 应用场景：
     * 1. 动态增长的数据缓冲
     * 2. 消息聚合时的缓冲区扩展
     * 3. 大数据块的处理
     * 
     * 实现说明：
     * 1. 检查现有缓冲区容量是否足够
     * 2. 如果不足，创建新的更大缓冲区
     * 3. 将现有数据复制到新缓冲区
     */
    public static ByteBuffer ensureCapacity(ByteBuffer existingBuffer, int newLength) {
        if (newLength > existingBuffer.capacity()) {
            // 创建新的更大容量的缓冲区
            ByteBuffer newBuffer = ByteBuffer.allocate(newLength);
            // 准备现有缓冲区以进行读取
            existingBuffer.flip();
            // 将数据复制到新缓冲区
            newBuffer.put(existingBuffer);
            return newBuffer;
        }
        return existingBuffer;
    }

    /**
     * 创建有序集合
     * 将可变参数列表中的元素添加到TreeSet中
     * 
     * @param elems 要添加的元素
     * @param <T> 元素类型，必须实现Comparable接口
     * @return 包含所有元素的有序集合
     * 
     * 应用场景：
     * 1. 需要自动排序的数据集合
     * 2. 去重和排序场景
     * 3. 有序数据的维护
     * 
     * 实现说明：
     * 1. 创建新的TreeSet实例
     * 2. 遍历并添加所有元素
     * 3. 返回填充后的集合
     */
    @SafeVarargs
    public static <T extends Comparable<T>> SortedSet<T> mkSortedSet(T... elems) {
        // 创建TreeSet用于自动排序
        SortedSet<T> result = new TreeSet<>();
        // 添加所有元素
        for (T elem : elems)
            result.add(elem);
        return result;
    }

    /**
     * 创建Map.Entry实例
     * 用于配合mkMap方法构建映射
     * 
     * @param k 键
     * @param v 值
     * @param <K> 键类型
     * @param <V> 值类型
     * @return 新的Map.Entry实例
     * 
     * 应用场景：
     * 1. 动态构建Map的键值对
     * 2. 临时存储键值关系
     * 3. 配合mkMap方法使用
     * 
     * 实现说明：
     * 使用AbstractMap.SimpleEntry创建简单的键值对实现
     */
    public static <K, V> Map.Entry<K, V> mkEntry(final K k, final V v) {
        return new AbstractMap.SimpleEntry<>(k, v);
    }

    /**
     * 从Entry数组创建映射
     * 保持输入顺序的LinkedHashMap实现
     * 
     * @param entries 键值对条目数组
     * @param <K> 键类型
     * @param <V> 值类型
     * @return 包含所有条目的映射
     * 
     * 应用场景：
     * 1. 需要保持插入顺序的映射创建
     * 2. 配置项的有序存储
     * 3. 键值对的批量处理
     * 
     * 实现说明：
     * 1. 创建LinkedHashMap保持顺序
     * 2. 遍历并添加所有条目
     * 3. 返回完整的映射
     */
    @SafeVarargs
    public static <K, V> Map<K, V> mkMap(final Map.Entry<K, V>... entries) {
        // 使用LinkedHashMap保持顺序
        final LinkedHashMap<K, V> result = new LinkedHashMap<>();
        // 添加所有条目
        for (final Map.Entry<K, V> entry : entries) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * 从字符串映射创建Properties对象
     * 
     * @param properties 包含属性的映射
     * @return 新的Properties实例
     * 
     * 应用场景：
     * 1. 配置文件的处理
     * 2. 系统属性的设置
     * 3. 应用程序参数的管理
     * 
     * 实现说明：
     * 1. 创建新的Properties实例
     * 2. 使用setProperty方法添加所有键值对
     * 3. 返回填充后的Properties
     */
    public static Properties mkProperties(final Map<String, String> properties) {
        // 创建新的Properties实例
        final Properties result = new Properties();
        // 添加所有属性
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            result.setProperty(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * 从对象映射创建Properties对象
     * 支持非字符串类型的值
     * 
     * @param properties 包含属性的映射
     * @return 新的Properties实例
     * 
     * 应用场景：
     * 1. 混合类型配置的处理
     * 2. 复杂配置项的管理
     * 3. 系统参数的动态设置
     * 
     * 实现说明：
     * 1. 创建新的Properties实例
     * 2. 使用put方法添加所有键值对
     * 3. 返回填充后的Properties
     */
    public static Properties mkObjectProperties(final Map<String, Object> properties) {
        // 创建新的Properties实例
        final Properties result = new Properties();
        // 添加所有属性
        for (final Map.Entry<String, Object> entry : properties.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * 递归删除给定的文件/目录及其所有子文件（如果存在）
     * 这个方法使用Java 7引入的Files.walkFileTree API来安全地遍历和删除文件树
     *
     * @param rootFile 要开始删除的根文件或目录
     * 
     * 应用场景：
     * 1. 清理临时文件和目录
     * 2. 删除日志文件及其归档
     * 3. 卸载或清理组件时的资源释放
     * 
     * 实现说明：
     * 1. 使用Files.walkFileTree进行文件树遍历
     * 2. 通过SimpleFileVisitor处理遍历过程中的各种情况
     * 3. 包含完整的错误处理和并发安全考虑
     */
    public static void delete(final File rootFile) throws IOException {
        // 空值检查，避免NPE
        if (rootFile == null)
            return;
        // 使用Files.walkFileTree进行文件树遍历，提供自定义的SimpleFileVisitor实现
        Files.walkFileTree(rootFile.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFileFailed(Path path, IOException exc) throws IOException {
                // 处理文件访问失败的情况
                if (exc instanceof NoSuchFileException) {
                    if (path.toFile().equals(rootFile)) {
                        // 如果根路径不存在，忽略错误并终止遍历
                        return FileVisitResult.TERMINATE;
                    } else {
                        // 如果是其他路径不存在，继续遍历，因为文件可能已被其他线程删除
                        return FileVisitResult.CONTINUE;
                    }
                }
                // 对于其他类型的IO异常，直接抛出
                throw exc;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                // 删除遍历到的文件，使用deleteIfExists避免文件不存在时的异常
                Files.deleteIfExists(path);
                // 继续遍历其他文件
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException exc) throws IOException {
                // KAFKA-8999: 如果之前的操作已经抛出异常，应该将其传播出去
                if (exc != null) {
                    throw exc;
                }

                // 在访问完目录的所有内容后删除目录本身
                Files.deleteIfExists(path);
                // 继续遍历其他目录
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * 如果提供的列表为null则返回空列表，否则返回列表本身
     * 这个方法用于处理可能为null的列表，避免出现NullPointerException
     *
     * @param other 要检查是否为null的列表
     * @return 如果输入列表为null则返回空列表，否则返回原列表
     * 
     * 应用场景：
     * 1. 处理可能为null的配置项列表
     * 2. 安全地处理外部输入的集合数据
     * 3. 在不确定列表是否初始化的情况下进行操作
     * 
     * 实现说明：
     * 1. 使用三元运算符进行null检查
     * 2. 使用Collections.emptyList()获取不可变的空列表
     * 3. 保持原列表不变，确保方法的无副作用性
     */
    public static <T> List<T> safe(List<T> other) {
        // 使用三元运算符，如果other为null则返回空列表，否则返回原列表
        return other == null ? Collections.emptyList() : other;
    }

    /**
     * 获取加载Kafka的类加载器
     * 这个方法返回加载Utils类的类加载器，通常用于确保使用正确的类加载器加载Kafka相关的类
     * 
     * 应用场景：
     * 1. 动态加载Kafka的插件或扩展
     * 2. 在自定义类加载器环境中确保正确加载Kafka类
     * 3. 处理类加载器隔离相关的问题
     * 
     * 实现说明：
     * 使用Utils.class.getClassLoader()获取加载当前类的类加载器
     */
    public static ClassLoader getKafkaClassLoader() {
        // 返回加载Utils类的类加载器
        return Utils.class.getClassLoader();
    }

    /**
     * 获取当前线程的上下文类加载器，如果不存在则返回加载Kafka的类加载器
     * 这个方法在需要使用Class.forName加载类时特别有用
     * 
     * @return 当前线程的上下文类加载器或Kafka的类加载器
     * 
     * 应用场景：
     * 1. 在多线程环境中确保正确的类加载
     * 2. 处理不同类加载器上下文之间的交互
     * 3. 动态加载类时提供合适的类加载器
     * 
     * 实现说明：
     * 1. 首先尝试获取当前线程的上下文类加载器
     * 2. 如果上下文类加载器为null，则使用Kafka的类加载器作为后备
     */
    public static ClassLoader getContextOrKafkaClassLoader() {
        // 获取当前线程的上下文类加载器
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null)
            // 如果上下文类加载器不存在，返回Kafka的类加载器
            return getKafkaClassLoader();
        else
            // 返回上下文类加载器
            return cl;
    }

    /**
     * 尝试原子方式移动源文件到目标位置，如果失败则回退到非原子移动
     * 该方法同时会刷新父目录以确保崩溃一致性
     *
     * @param source 源文件路径
     * @param target 目标文件路径
     * @throws IOException 如果原子移动和非原子移动都失败，或父目录刷新失败
     * 
     * 应用场景：
     * 1. 日志文件的安全轮转
     * 2. 配置文件的原子更新
     * 3. 需要保证文件操作一致性的场景
     * 
     * 实现说明：
     * 调用三参数版本的方法，并设置needFlushParentDir为true
     */
    public static void atomicMoveWithFallback(Path source, Path target) throws IOException {
        // 调用完整版本的方法，确保刷新父目录
        atomicMoveWithFallback(source, target, true);
    }

    /**
     * 尝试原子方式移动源文件到目标位置，如果失败则回退到非原子移动
     * 该方法允许调用者决定是否刷新父目录，这在对同一目录进行多次原子移动操作时很有用，
     * 可以避免重复刷新相同的父目录
     *
     * @param source 源文件路径
     * @param target 目标文件路径
     * @param needFlushParentDir 是否需要刷新父目录
     * @throws IOException 如果原子移动和非原子移动都失败，或当needFlushParentDir为true时父目录刷新失败
     * 
     * 应用场景：
     * 1. 批量文件更新时的性能优化
     * 2. 需要精细控制目录刷新时机的场景
     * 3. 文件系统事务操作
     * 
     * 实现说明：
     * 1. 首先尝试原子移动操作
     * 2. 如果原子移动失败，回退到普通移动
     * 3. 根据参数决定是否刷新父目录
     * 4. 完整的异常处理和日志记录
     */
    public static void atomicMoveWithFallback(Path source, Path target, boolean needFlushParentDir) throws IOException {
        try {
            // 尝试原子方式移动文件
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException outer) {
            try {
                // 原子移动失败，记录警告日志
                log.warn("Failed atomic move of {} to {} retrying with a non-atomic move", source, target, outer);
                // 尝试非原子方式移动文件，允许覆盖已存在的目标文件
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                // 非原子移动成功，记录调试日志
                log.debug("Non-atomic move of {} to {} succeeded after atomic move failed", source, target);
            } catch (IOException inner) {
                // 如果非原子移动也失败，将原始异常添加为被抑制的异常
                inner.addSuppressed(outer);
                throw inner;
            }
        } finally {
            if (needFlushParentDir) {
                // 如果需要，刷新目标文件的父目录以确保文件系统元数据的一致性
                flushDir(target.toAbsolutePath().normalize().getParent());
            }
        }
    }

    /**
     * 刷新脏目录以保证崩溃一致性。
     * <p>
     * 注意：在Windows操作系统上不执行fsync操作，因为这会导致AccessDeniedException异常（KAFKA-13391）
     *
     * 应用场景：
     * 1. 数据目录的持久化
     * 2. 日志目录的一致性保证
     * 3. 配置文件目录的同步
     *
     * 实现说明：
     * 1. 首先检查路径是否为null且不是Windows或Z/OS系统
     * 2. 以只读方式打开目录对应的FileChannel
     * 3. 调用force方法强制将目录的元数据刷新到磁盘
     *
     * @throws IOException 如果刷新目录失败
     */
    public static void flushDir(Path path) throws IOException {
        // 检查路径是否有效且不是Windows或Z/OS系统
        if (path != null && !OperatingSystem.IS_WINDOWS && !OperatingSystem.IS_ZOS) {
            // 使用try-with-resources确保FileChannel正确关闭
            try (FileChannel dir = FileChannel.open(path, StandardOpenOption.READ)) {
                // 强制刷新目录元数据到磁盘，参数true表示同时刷新文件内容和元数据
                dir.force(true);
            }
        }
    }

    /**
     * 刷新脏目录以保证崩溃一致性，同时处理目录不存在的情况。
     * 这是一个对flushDir方法的包装，提供了更好的异常处理机制。
     *
     * 应用场景：
     * 1. 在不确定目录是否存在的情况下进行刷新操作
     * 2. 需要静默处理目录不存在异常的场景
     * 3. 批量目录同步时的健壮性处理
     *
     * 实现说明：
     * 1. 调用基础的flushDir方法
     * 2. 捕获并记录NoSuchFileException异常
     * 3. 使用警告级别记录日志
     *
     * @throws IOException 如果刷新目录失败（不包括文件不存在的情况）
     */
    public static void flushDirIfExists(Path path) throws IOException {
        try {
            // 尝试刷新目录
            flushDir(path);
        } catch (NoSuchFileException e) {
            // 如果目录不存在，记录警告日志
            log.warn("Failed to flush directory {}", path);
        }
    }

    /**
     * 刷新文件内容到磁盘，同时处理文件不存在的情况。
     * 这个方法用于确保文件数据的持久化，即使文件不存在也不会抛出异常。
     *
     * 应用场景：
     * 1. 日志文件的持久化
     * 2. 配置文件的同步
     * 3. 临时文件的刷新
     *
     * 实现说明：
     * 1. 以只读方式打开文件通道
     * 2. 强制刷新文件内容到磁盘
     * 3. 优雅处理文件不存在的情况
     */
    public static void flushFileIfExists(Path path) throws IOException {
        // 使用try-with-resources确保FileChannel正确关闭
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            // 强制刷新文件内容到磁盘
            fileChannel.force(true);
        } catch (NoSuchFileException e) {
            // 如果文件不存在，记录警告日志
            log.warn("Failed to flush file {}", path, e);
        }
    }

    /**
     * 关闭所有提供的可关闭资源。
     * 这个方法会尝试关闭所有资源，并收集所有异常。
     *
     * 应用场景：
     * 1. 批量关闭数据库连接
     * 2. 清理文件句柄
     * 3. 网络连接的释放
     *
     * 实现说明：
     * 1. 遍历所有资源并尝试关闭
     * 2. 收集第一个异常，后续异常作为被抑制的异常
     * 3. 最后抛出收集到的异常（如果有）
     *
     * @throws IOException 如果任何close方法抛出IOException。
     *         第一个IOException会被抛出，后续异常会被添加为被抑制的异常。
     */
    public static void closeAll(Closeable... closeables) throws IOException {
        // 用于存储第一个遇到的异常
        IOException exception = null;
        // 遍历所有可关闭的资源
        for (Closeable closeable : closeables) {
            try {
                // 如果资源不为null则关闭
                if (closeable != null)
                    closeable.close();
            } catch (IOException e) {
                if (exception != null)
                    // 如果已经有异常，则将新异常添加为被抑制的异常
                    exception.addSuppressed(e);
                else
                    // 记录第一个异常
                    exception = e;
            }
        }
        // 如果有异常发生，则抛出
        if (exception != null)
            throw exception;
    }

    /**
     * 定义一个函数式接口，用于执行可能抛出异常的操作。
     * 这个接口主要用于异常处理和资源清理的场景。
     */
    @FunctionalInterface
    public interface SwallowAction {
        /**
         * 执行操作的方法，可能抛出任何异常
         * @throws Throwable 执行过程中可能抛出的任何异常
         */
        void run() throws Throwable;
    }

    /**
     * 执行给定的代码，并按指定的日志级别处理异常。
     * 这是一个便捷方法，用于执行可能抛出异常的代码。
     *
     * @param log 用于记录异常的日志器
     * @param level 日志记录的级别
     * @param what 描述正在执行的操作的消息
     * @param code 要执行的代码
     */
    public static void swallow(final Logger log, final Level level, final String what, final SwallowAction code) {
        // 调用完整版本的swallow方法，不记录第一个异常
        swallow(log, level, what, code, null);
    }

    /**
     * 执行给定的代码，并处理可能发生的异常。
     * 这个方法提供了灵活的异常处理机制，可以选择记录异常并保存第一个发生的异常。
     *
     * 应用场景：
     * 1. 执行清理操作时的异常处理
     * 2. 资源释放时的错误处理
     * 3. 需要记录但不中断执行的操作
     *
     * 实现说明：
     * 1. 执行提供的代码
     * 2. 捕获所有可能的异常
     * 3. 根据指定的日志级别记录异常
     * 4. 可选地保存第一个发生的异常
     */
    public static void swallow(final Logger log, final Level level, final String what, final SwallowAction code,
                               final AtomicReference<Throwable> firstException) {
        if (code != null) {
            try {
                // 执行提供的代码
                code.run();
            } catch (Throwable t) {
                // 根据指定的日志级别记录异常
                switch (level) {
                    case INFO:
                        log.info(what, t);
                        break;
                    case DEBUG:
                        log.debug(what, t);
                        break;
                    case ERROR:
                        log.error(what, t);
                        break;
                    case TRACE:
                        log.trace(what, t);
                        break;
                    case WARN:
                    default:
                        log.warn(what, t);
                }
                // 如果提供了firstException引用，则保存第一个异常
                if (firstException != null)
                    firstException.compareAndSet(null, t);
            }
        }
    }

    /**
     * 一个不带throws子句的AutoCloseable接口。
     * 这个接口主要用于简化lambda表达式中的异常处理。
     *
     * 应用场景：
     * 1. try-with-resources语句中使用lambda表达式
     * 2. 避免不必要的检查异常转换
     * 3. 简化资源清理代码
     */
    @FunctionalInterface
    public interface UncheckedCloseable extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * 如果对象实现了AutoCloseable接口，则安全地关闭它。
     * 这个方法提供了一种通用的方式来处理可能是可关闭资源的对象。
     *
     * 应用场景：
     * 1. 处理类型未知的资源对象
     * 2. 通用资源清理代码
     * 3. 防御性编程中的资源处理
     *
     * @param maybeCloseable 可能是可关闭资源的对象
     * @param name 资源的描述性名称，用于日志记录
     */
    public static void maybeCloseQuietly(Object maybeCloseable, String name) {
        // 检查对象是否实现了AutoCloseable接口
        if (maybeCloseable instanceof AutoCloseable)
            // 如果是，则调用closeQuietly方法关闭它
            closeQuietly((AutoCloseable) maybeCloseable, name);
    }

    /**
     * 安静地关闭一个AutoCloseable对象，异常会以警告级别记录。
     * 
     * 应用场景：
     * 1. 资源清理代码
     * 2. finally块中的资源释放
     * 3. 应用程序关闭时的清理
     *
     * 注意事项：
     * 使用方法引用作为参数时要特别小心，例如：
     * {@code closeQuietly(task::stop, "source task");}
     * 虽然此方法能优雅处理null的AutoCloseable对象，
     * 但从null对象获取方法引用会导致NullPointerException。
     * 调用者需要确保传入方法引用的对象不为null。
     */
    public static void closeQuietly(AutoCloseable closeable, String name) {
        // 使用默认日志器调用完整版本的closeQuietly方法
        closeQuietly(closeable, name, log);
    }

    /**
     * 使用指定的日志器安静地关闭一个AutoCloseable对象。
     * 这个方法提供了更灵活的日志记录选项。
     *
     * 应用场景：
     * 1. 需要使用特定日志器的资源清理
     * 2. 自定义日志记录的资源关闭
     * 3. 特定组件的资源管理
     *
     * 注意事项：
     * 使用方法引用作为参数时要特别小心，例如：
     * {@code closeQuietly(task::stop, "source task");}
     * 虽然此方法能优雅处理null的AutoCloseable对象，
     * 但从null对象获取方法引用会导致NullPointerException。
     * 调用者需要确保传入方法引用的对象不为null。
     */
    public static void closeQuietly(AutoCloseable closeable, String name, Logger logger) {
        if (closeable != null) {
            try {
                // 尝试关闭资源
                closeable.close();
            } catch (Throwable t) {
                // 使用提供的日志器记录警告信息
                logger.warn("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
            }
        }
    }

    /**
     * 安静地关闭一个AutoCloseable对象，并记录第一个发生的异常。
     * 这个方法适用于需要跟踪错误但不想中断执行的场景。
     *
     * 应用场景：
     * 1. 需要保留异常信息的资源清理
     * 2. 错误追踪和诊断
     * 3. 批量资源清理时的异常管理
     *
     * 注意事项：
     * 使用方法引用作为参数时要特别小心，例如：
     * {@code closeQuietly(task::stop, "source task");}
     * 虽然此方法能优雅处理null的AutoCloseable对象，
     * 但从null对象获取方法引用会导致NullPointerException。
     * 调用者需要确保传入方法引用的对象不为null。
     */
    public static void closeQuietly(AutoCloseable closeable, String name, AtomicReference<Throwable> firstException) {
        if (closeable != null) {
            try {
                // 尝试关闭资源
                closeable.close();
            } catch (Throwable t) {
                // 原子地设置第一个异常
                firstException.compareAndSet(null, t);
                // 记录错误日志
                log.error("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
            }
        }
    }

    /**
     * 安静地关闭多个AutoCloseable对象，即使其中某些对象抛出异常。
     * 这个方法提供了批量资源清理的能力，同时保持了良好的异常处理机制。
     *
     * 应用场景：
     * 1. 批量关闭数据库连接
     * 2. 清理多个文件句柄
     * 3. 应用程序关闭时的资源清理
     *
     * 实现说明：
     * 遍历所有资源，使用closeQuietly方法关闭每个资源，
     * 同时保持对第一个异常的追踪
     *
     * @param firstException 用于保存第一个发生的异常
     * @param name 资源的描述性名称
     * @param closeables 要关闭的资源数组
     */
    public static void closeAllQuietly(AtomicReference<Throwable> firstException, String name, AutoCloseable... closeables) {
        // 遍历所有资源并安静地关闭它们
        for (AutoCloseable closeable : closeables) closeQuietly(closeable, name, firstException);
    }

    /**
     * 批量执行函数列表，即使其中某些函数抛出异常也会继续执行
     * <p>
     * 如果任何函数抛出异常，第一个异常将在最后被重新抛出，后续异常将作为被抑制的异常添加到第一个异常中
     * 
     * 应用场景：
     * 1. 批量关闭资源（如数据库连接、文件句柄等）
     * 2. 执行多个清理操作
     * 3. 批量执行可能失败的任务，但需要确保所有任务都被尝试执行
     * 
     * 实现说明：
     * 1. 使用List<Callable<Void>>作为参数，支持批量执行任务
     * 2. 通过异常链实现异常信息的完整保留
     * 3. 保证所有任务都会被执行，不会因为某个任务失败而中断
     */
    // 注意：这是closeAll方法的通用版本。我们可以通过修改签名为public <R> List<R> tryAll(all: List[Callable<R>])使其更通用
    public static void tryAll(List<Callable<Void>> all) throws Throwable {
        // 用于保存第一个遇到的异常
        Throwable exception = null;
        // 遍历并执行所有任务
        for (Callable<Void> call : all) {
            try {
                call.call();
            } catch (Throwable t) {
                if (exception != null)
                    // 如果已经有异常，将当前异常添加为被抑制的异常
                    exception.addSuppressed(t);
                else
                    // 记录第一个异常
                    exception = t;
            }
        }
        // 如果有异常发生，抛出第一个异常（包含所有被抑制的异常）
        if (exception != null)
            throw exception;
    }

    /**
     * 将数字转换为正值的快速方法
     * 当输入为正数时，返回原值；当输入为负数时，通过与0x7fffffff进行按位与操作返回一个正值（注意：这不是其绝对值）
     * 
     * 应用场景：
     * 1. 生产者的分区选择逻辑中用于确定分区号
     * 2. 需要快速获取正值且不关心具体数值的场景
     * 3. 哈希值的正值转换
     * 
     * 实现说明：
     * 1. 使用位运算代替取绝对值，提高性能
     * 2. 0x7fffffff是int类型的最大正值的二进制表示
     * 3. 该方法保证了结果的确定性，相同的输入总是产生相同的输出
     * 
     * 注意：由于该方法在生产者的分区选择逻辑中使用，修改此方法可能导致与现有分区上的消息不兼容
     * {@link org.apache.kafka.clients.producer.KafkaProducer}
     *
     * @param number 输入的数字
     * @return 转换后的正数
     */
    public static int toPositive(int number) {
        // 通过与0x7fffffff进行按位与操作，确保结果为正数
        return number & 0x7fffffff;
    }

    /**
     * 从给定偏移量开始读取带大小前缀的字节缓冲区
     * 
     * 应用场景：
     * 1. 读取消息格式中的变长字段
     * 2. 处理网络协议中的长度前缀数据
     * 3. 反序列化带大小信息的数据结构
     * 
     * 实现说明：
     * 1. 首先读取4字节的大小信息
     * 2. 根据大小信息创建新的ByteBuffer视图
     * 3. 通过位置调整和切片操作提取数据部分
     * 
     * @param buffer 包含大小信息和数据的缓冲区
     * @param start 开始读取的偏移量
     * @return 仅包含数据部分的缓冲区切片（不包括大小信息），如果大小为负数则返回null
     */
    public static ByteBuffer sizeDelimited(ByteBuffer buffer, int start) {
        // 读取大小信息（4字节整数）
        int size = buffer.getInt(start);
        if (size < 0) {
            // 如果大小为负数，返回null表示无效数据
            return null;
        } else {
            // 创建缓冲区的副本，避免影响原始缓冲区
            ByteBuffer b = buffer.duplicate();
            // 将位置设置到数据开始处（跳过大小信息）
            b.position(start + 4);
            // 创建新的切片，只包含数据部分
            b = b.slice();
            // 设置限制为数据大小
            b.limit(size);
            // 重置位置到开始处
            b.rewind();
            return b;
        }
    }

    /**
     * 从文件通道读取数据到目标缓冲区，直到缓冲区被完全填满
     * 如果在填满缓冲区之前达到文件末尾，则抛出EOFException
     * 
     * 应用场景：
     * 1. 读取固定大小的文件头或元数据
     * 2. 验证文件完整性
     * 3. 读取需要严格控制大小的数据块
     * 
     * 实现说明：
     * 1. 验证起始位置的有效性
     * 2. 调用readFully方法读取数据
     * 3. 检查是否完全读取，如果未完全读取则抛出异常
     *
     * @param channel 要读取的文件通道
     * @param destinationBuffer 用于存储读取数据的缓冲区
     * @param position 开始读取的文件位置（必须非负）
     * @param description 正在读取的内容描述，用于异常信息
     *
     * @throws IllegalArgumentException 如果position为负数
     * @throws EOFException 如果在填满缓冲区之前达到文件末尾
     * @throws IOException 如果发生I/O错误
     */
    public static void readFullyOrFail(FileChannel channel, ByteBuffer destinationBuffer, long position,
                                       String description) throws IOException {
        // 验证位置参数的有效性
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        // 记录期望读取的字节数
        int expectedReadBytes = destinationBuffer.remaining();
        // 尝试读取数据
        readFully(channel, destinationBuffer, position);
        // 检查是否完全读取
        if (destinationBuffer.hasRemaining()) {
            throw new EOFException(String.format("Failed to read `%s` from file channel `%s`. Expected to read %d bytes, " +
                    "but reached end of file after reading %d bytes. Started read from position %d.",
                    description, channel, expectedReadBytes, expectedReadBytes - destinationBuffer.remaining(), position));
        }
    }

    /**
     * 从文件通道读取数据到目标缓冲区，直到缓冲区被填满或达到文件末尾
     * 
     * 应用场景：
     * 1. 读取可能不完整的数据块
     * 2. 流式处理文件内容
     * 3. 读取变长数据结构
     * 
     * 实现说明：
     * 1. 验证起始位置的有效性
     * 2. 循环读取数据直到条件满足
     * 3. 累计读取的字节数并更新位置
     *
     * @param channel 要读取的文件通道
     * @param destinationBuffer 用于存储读取数据的缓冲区
     * @param position 开始读取的文件位置（必须非负）
     *
     * @throws IllegalArgumentException 如果position为负数
     * @throws IOException 如果发生I/O错误
     */
    public static void readFully(FileChannel channel, ByteBuffer destinationBuffer, long position) throws IOException {
        // 验证位置参数的有效性
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        // 记录当前读取位置
        long currentPosition = position;
        int bytesRead;
        // 循环读取直到达到文件末尾或缓冲区已满
        do {
            bytesRead = channel.read(destinationBuffer, currentPosition);
            currentPosition += bytesRead;
        } while (bytesRead != -1 && destinationBuffer.hasRemaining());
    }

    /**
     * 从输入流读取数据到目标缓冲区，直到缓冲区被填满或达到流末尾
     * 
     * 应用场景：
     * 1. 网络数据的读取和处理
     * 2. 文件流的读取操作
     * 3. 处理可能不完整的数据流
     * 
     * 实现说明：
     * 1. 确保目标缓冲区有底层数组支持
     * 2. 计算正确的数组偏移量
     * 3. 循环读取数据直到条件满足
     *
     * @param inputStream 要读取的输入流
     * @param destinationBuffer 用于存储读取数据的缓冲区（必须有数组支持）
     * @return 实际读取的字节数
     * @throws IOException 如果发生I/O错误
     */
    public static int readFully(InputStream inputStream, ByteBuffer destinationBuffer) throws IOException {
        // 验证缓冲区是否有数组支持
        if (!destinationBuffer.hasArray())
            throw new IllegalArgumentException("destinationBuffer must be backed by an array");
        // 计算初始偏移量
        int initialOffset = destinationBuffer.arrayOffset() + destinationBuffer.position();
        // 获取底层数组
        byte[] array = destinationBuffer.array();
        // 获取剩余可读取的长度
        int length = destinationBuffer.remaining();
        // 记录总共读取的字节数
        int totalBytesRead = 0;
        // 循环读取直到达到预期长度或流结束
        do {
            int bytesRead = inputStream.read(array, initialOffset + totalBytesRead, length - totalBytesRead);
            if (bytesRead == -1)
                break;
            totalBytesRead += bytesRead;
        } while (length > totalBytesRead);
        // 更新缓冲区的位置
        destinationBuffer.position(destinationBuffer.position() + totalBytesRead);
        return totalBytesRead;
    }

    /**
     * 将源缓冲区的所有剩余数据写入文件通道
     * 
     * 应用场景：
     * 1. 文件数据的写入操作
     * 2. 缓冲区数据的持久化
     * 3. 网络数据的发送
     * 
     * 实现说明：
     * 循环写入直到缓冲区中没有剩余数据
     * 
     * @param channel 要写入的文件通道
     * @param sourceBuffer 包含要写入数据的源缓冲区
     * @throws IOException 如果发生I/O错误
     */
    public static void writeFully(FileChannel channel, ByteBuffer sourceBuffer) throws IOException {
        // 循环写入直到没有剩余数据
        while (sourceBuffer.hasRemaining())
            channel.write(sourceBuffer);
    }

    /**
     * 尝试将源缓冲区的数据写入到可传输通道中
     * 由于写入操作可能不会一次完成，可能需要多次调用此方法才能将源缓冲区的数据完全写入目标通道
     *
     * @param destChannel 目标传输通道
     * @param position 源缓冲区开始写入的位置
     * @param length 最大可写入的字节数
     * @param sourceBuffer 源缓冲区
     *
     * @return 实际写入的字节数
     * @throws IOException 如果发生I/O错误
     * 
     * 应用场景：
     * 1. 网络数据传输
     * 2. 文件写入操作
     * 3. 大数据块的分段传输
     * 
     * 实现说明：
     * 1. 创建源缓冲区的副本以避免影响原始缓冲区的位置和限制
     * 2. 设置副本缓冲区的位置和限制以定义写入范围
     * 3. 将数据写入目标通道并返回实际写入的字节数
     */
    public static int tryWriteTo(TransferableChannel destChannel,
                                  int position,
                                  int length,
                                  ByteBuffer sourceBuffer) throws IOException {
        // 创建源缓冲区的副本，避免修改原始缓冲区的状态
        ByteBuffer dup = sourceBuffer.duplicate();
        // 设置写入的起始位置
        dup.position(position);
        // 设置写入的结束位置
        dup.limit(position + length);
        // 执行写入操作并返回实际写入的字节数
        return destChannel.write(dup);
    }

    /**
     * 将缓冲区的内容写入输出流
     * 从缓冲区的当前位置开始复制字节数据
     * 
     * @param out 要写入的输出流
     * @param buffer 要读取的缓冲区
     * @param length 要写入的字节数
     * @throws IOException 写入输出流时发生的任何错误
     * 
     * 应用场景：
     * 1. 将内存中的数据写入文件
     * 2. 网络数据传输
     * 3. 数据序列化
     * 
     * 实现说明：
     * 1. 检查缓冲区是否有底层数组，有则直接写入
     * 2. 没有底层数组时，逐字节读取并写入
     */
    public static void writeTo(DataOutput out, ByteBuffer buffer, int length) throws IOException {
        if (buffer.hasArray()) {
            // 如果缓冲区有底层数组，直接批量写入以提高性能
            out.write(buffer.array(), buffer.position() + buffer.arrayOffset(), length);
        } else {
            // 如果是直接缓冲区，需要逐字节读取并写入
            int pos = buffer.position();
            for (int i = pos; i < length + pos; i++)
                out.writeByte(buffer.get(i));
        }
    }

    /**
     * 将Iterable对象转换为List
     * 
     * @param iterable 要转换的Iterable对象
     * @param <T> 集合元素类型
     * @return 包含所有元素的List
     * 
     * 应用场景：
     * 1. 将Set等集合转换为List
     * 2. 将自定义Iterable实现转换为标准List
     * 
     * 实现说明：
     * 通过调用toList(iterator)方法完成转换
     */
    public static <T> List<T> toList(Iterable<T> iterable) {
        return toList(iterable.iterator());
    }

    /**
     * 将Iterator转换为List
     * 
     * @param iterator 要转换的Iterator
     * @param <T> 集合元素类型
     * @return 包含所有元素的List
     * 
     * 应用场景：
     * 1. 将迭代器转换为可随机访问的列表
     * 2. 将流式处理结果收集为List
     * 
     * 实现说明：
     * 1. 创建新的ArrayList
     * 2. 遍历迭代器并添加所有元素
     */
    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> res = new ArrayList<>();
        while (iterator.hasNext())
            res.add(iterator.next());
        return res;
    }

    /**
     * 将Iterator转换为List，并使用谓词过滤元素
     * 
     * @param iterator 要转换的Iterator
     * @param predicate 用于过滤元素的谓词
     * @param <T> 集合元素类型
     * @return 包含所有满足谓词条件的元素的List
     * 
     * 应用场景：
     * 1. 在转换为List的同时进行元素过滤
     * 2. 创建满足特定条件的子集
     * 
     * 实现说明：
     * 1. 创建新的ArrayList
     * 2. 遍历迭代器
     * 3. 对每个元素应用谓词测试
     * 4. 只添加满足条件的元素
     */
    public static <T> List<T> toList(Iterator<T> iterator, Predicate<T> predicate) {
        List<T> res = new ArrayList<>();
        while (iterator.hasNext()) {
            T e = iterator.next();
            if (predicate.test(e)) {
                res.add(e);
            }
        }
        return res;
    }

    /**
     * 将字节集合转换为32位整数字段
     * 每个字节值对应结果中的一个位，通过位运算设置对应位置的值
     * 
     * @param bytes 要转换的字节集合
     * @return 转换后的32位整数
     * 
     * 应用场景：
     * 1. 将多个布尔标志压缩存储
     * 2. 实现位图索引
     * 3. 权限标志位的设置
     * 
     * 实现说明：
     * 1. 初始化结果值为0
     * 2. 遍历字节集合
     * 3. 对每个字节值进行范围检查
     * 4. 通过位移和或运算设置对应位
     */
    public static int to32BitField(final Set<Byte> bytes) {
        int value = 0;
        for (final byte b : bytes)
            // 将1左移b位，然后与value进行或运算，设置对应位为1
            value |= 1 << checkRange(b);
        return value;
    }

    /**
     * 检查字节值是否在有效范围内（0-31）
     * 
     * @param i 要检查的字节值
     * @return 如果在有效范围内则返回原值
     * @throws IllegalArgumentException 如果值超出范围
     * 
     * 实现说明：
     * 1. 检查是否大于31
     * 2. 检查是否小于0
     * 3. 在范围内则返回原值
     */
    private static byte checkRange(final byte i) {
        if (i > 31)
            throw new IllegalArgumentException("out of range: i>31, i = " + i);
        if (i < 0)
            throw new IllegalArgumentException("out of range: i<0, i = " + i);
        return i;
    }

    /**
     * 将32位整数字段转换回字节集合
     * 通过检查每一位是否为1来重建原始的字节集合
     * 
     * @param intValue 要转换的32位整数
     * @return 包含所有设置位对应值的字节集合
     * 
     * 应用场景：
     * 1. 解析位图索引
     * 2. 读取压缩的布尔标志
     * 3. 权限检查
     * 
     * 实现说明：
     * 1. 创建结果集合
     * 2. 通过位移和与运算检查每一位
     * 3. 将设置为1的位的位置添加到结果集合
     */
    public static Set<Byte> from32BitField(final int intValue) {
        Set<Byte> result = new HashSet<>();
        // 使用无符号右移和位与运算逐位检查
        for (int itr = intValue, count = 0; itr != 0; itr >>>= 1) {
            if ((itr & 1) != 0)
                result.add((byte) count);
            count++;
        }
        return result;
    }

    /**
     * 创建一个用于将Map.Entry流转换为指定类型Map的收集器
     * 提供两个主要便利功能：
     * 1. 可以指定返回Map的具体类型
     * 2. 可以直接将Entry流转换为Map，无需单独指定键值函数
     * 
     * @param mapSupplier 具体Map类型的构造器
     * @param <K> Map键的类型
     * @param <V> Map值的类型
     * @param <M> Map本身的类型
     * @return 新的Entry收集器
     * 
     * 应用场景：
     * 1. 对Map条目进行过滤后重新收集为新Map
     * 2. 将Entry流转换为特定类型的Map
     * 3. 自定义Map的构建过程
     * 
     * 实现说明：
     * 1. 创建自定义Collector实现
     * 2. 实现所有必要的收集器方法
     * 3. 设置收集器的特征
     */
    public static <K, V, M extends Map<K, V>> Collector<Map.Entry<K, V>, M, M> entriesToMap(final Supplier<M> mapSupplier) {
        return new Collector<Map.Entry<K, V>, M, M>() {
            @Override
            public Supplier<M> supplier() {
                // 提供新的Map实例
                return mapSupplier;
            }

            @Override
            public BiConsumer<M, Map.Entry<K, V>> accumulator() {
                // 定义如何将新的Entry添加到Map中
                return (map, entry) -> map.put(entry.getKey(), entry.getValue());
            }

            @Override
            public BinaryOperator<M> combiner() {
                // 定义如何合并两个Map
                return (map, map2) -> {
                    map.putAll(map2);
                    return map;
                };
            }

            @Override
            public Function<M, M> finisher() {
                // 不需要最终转换，直接返回收集的Map
                return map -> map;
            }

            @Override
            public Set<Characteristics> characteristics() {
                // 设置收集器的特征：无序且不需要最终转换
                return EnumSet.of(Characteristics.UNORDERED, Characteristics.IDENTITY_FINISH);
            }
        };
    }

    /**
     * 计算多个集合的并集
     * 该方法使用提供的构造器创建一个新的集合，并将所有输入集合的元素添加到结果集合中
     * 
     * @param constructor 用于创建结果集合的构造器，允许调用者指定具体的Set实现类型
     * @param set 要计算并集的集合数组
     * @param <E> 集合元素的类型
     * @return 包含所有输入集合元素的并集
     * 
     * 应用场景：
     * 1. 合并多个配置项集合
     * 2. 整合多个用户权限集合
     * 3. 合并多个主题分区列表
     * 
     * 实现说明：
     * 1. 使用提供的构造器创建新的结果集合
     * 2. 遍历所有输入集合，将每个集合的所有元素添加到结果集合中
     * 3. 返回最终的并集结果
     * 
     * 性能考虑：
     * - 时间复杂度：O(n)，其中n为所有输入集合的元素总数
     * - 空间复杂度：O(m)，其中m为并集后的元素总数
     */
    @SafeVarargs
    public static <E> Set<E> union(final Supplier<Set<E>> constructor, final Set<E>... set) {
        // 使用构造器创建新的结果集合
        final Set<E> result = constructor.get();
        // 遍历所有输入集合
        for (final Set<E> s : set) {
            // 将当前集合的所有元素添加到结果集合中
            result.addAll(s);
        }
        // 返回并集结果
        return result;
    }

    /**
     * 计算多个集合的交集
     * 该方法首先将第一个集合的所有元素添加到结果集合中，然后与其他集合逐个求交集
     * 
     * @param constructor 用于创建结果集合的构造器，允许调用者指定具体的Set实现类型
     * @param first 第一个集合，作为初始结果集
     * @param set 其他要参与交集计算的集合数组
     * @param <E> 集合元素的类型
     * @return 包含所有输入集合共有元素的交集
     * 
     * 应用场景：
     * 1. 查找多个用户组共同的权限
     * 2. 确定多个主题分区的共同订阅者
     * 3. 寻找多个配置集合的共同配置项
     * 
     * 实现说明：
     * 1. 使用提供的构造器创建新的结果集合
     * 2. 将第一个集合的所有元素添加到结果集合中
     * 3. 遍历其他集合，每次使用retainAll保留共同元素
     * 4. 返回最终的交集结果
     * 
     * 性能考虑：
     * - 时间复杂度：O(n * m)，其中n为集合个数，m为最小集合的元素个数
     * - 空间复杂度：O(k)，其中k为第一个集合的元素个数
     */
    @SafeVarargs
    public static <E> Set<E> intersection(final Supplier<Set<E>> constructor, final Set<E> first, final Set<E>... set) {
        // 使用构造器创建新的结果集合
        final Set<E> result = constructor.get();
        // 将第一个集合的所有元素添加到结果集合中
        result.addAll(first);
        // 遍历其他集合
        for (final Set<E> s : set) {
            // 保留当前集合与结果集合的共同元素
            result.retainAll(s);
        }
        // 返回交集结果
        return result;
    }

    /**
     * 计算两个集合的差集
     * 该方法返回存在于第一个集合但不存在于第二个集合中的所有元素
     * 
     * @param constructor 用于创建结果集合的构造器，允许调用者指定具体的Set实现类型
     * @param left 作为被减数的集合
     * @param right 作为减数的集合
     * @param <E> 集合元素的类型
     * @return 包含仅在left集合中存在的元素的差集
     * 
     * 应用场景：
     * 1. 查找已删除的配置项（新配置与旧配置的差集）
     * 2. 识别未授权的操作（用户权限与已授权操作的差集）
     * 3. 查找未分配的资源（总资源与已分配资源的差集）
     * 
     * 实现说明：
     * 1. 使用提供的构造器创建新的结果集合
     * 2. 将left集合的所有元素添加到结果集合中
     * 3. 从结果集合中移除right集合中的所有元素
     * 4. 返回差集结果
     * 
     * 性能考虑：
     * - 时间复杂度：O(n + m)，其中n为left集合元素个数，m为right集合元素个数
     * - 空间复杂度：O(n)，其中n为left集合的元素个数
     */
    public static <E> Set<E> diff(final Supplier<Set<E>> constructor, final Set<E> left, final Set<E> right) {
        // 使用构造器创建新的结果集合
        final Set<E> result = constructor.get();
        // 将left集合的所有元素添加到结果集合中
        result.addAll(left);
        // 从结果集合中移除right集合中的所有元素
        result.removeAll(right);
        // 返回差集结果
        return result;
    }

    /**
     * 根据指定的谓词条件过滤Map中的元素
     * 该方法使用Stream API对Map进行过滤，返回满足条件的键值对组成的新Map
     * 
     * @param map 要进行过滤的原始Map
     * @param filterPredicate 用于过滤的谓词条件，对Map.Entry进行判断
     * @param <K> Map的键类型
     * @param <V> Map的值类型
     * @return 包含所有满足过滤条件的键值对的新Map
     * 
     * 应用场景：
     * 1. 过滤配置项Map中的特定配置
     * 2. 筛选符合条件的主题分区配置
     * 3. 清理过期或无效的缓存项
     * 
     * 实现说明：
     * 1. 将Map转换为Entry的Stream
     * 2. 使用filter方法应用过滤谓词
     * 3. 使用Collectors.toMap收集结果到新Map
     * 
     * 性能考虑：
     * - 时间复杂度：O(n)，其中n为Map中的键值对数量
     * - 空间复杂度：O(m)，其中m为过滤后的键值对数量
     * - 使用Stream API提供了良好的可读性和可维护性
     */
    public static <K, V> Map<K, V> filterMap(final Map<K, V> map, final Predicate<Entry<K, V>> filterPredicate) {
        // 将Map转换为Stream，应用过滤条件，并收集结果
        return map.entrySet().stream()
            .filter(filterPredicate)  // 应用过滤谓词
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));  // 收集结果到新Map
    }

    /**
     * 将Properties对象转换为Map对象
     * 所有的key必须是字符串类型，否则会抛出ConfigException异常
     * 
     * @param properties 要转换的Properties对象
     * @return 包含所有属性的Map对象
     * 
     * 应用场景：
     * 1. 配置文件解析后的属性转换
     * 2. 系统属性的类型安全转换
     * 3. 外部配置的标准化处理
     * 
     * 实现说明：
     * 通过调用castToStringObjectMap方法完成实际的转换工作
     */
    public static Map<String, Object> propsToMap(Properties properties) {
        return castToStringObjectMap(properties);
    }

    /**
     * 将任意类型键的Map转换为String类型键的Map
     * 
     * @param inputMap 具有任意类型键的Map
     * @return 具有相同内容但键类型为String的新Map
     * @throws ConfigException 如果任何键不是String类型
     * 
     * 应用场景：
     * 1. 配置数据的标准化处理
     * 2. 外部数据到内部数据结构的转换
     * 3. API参数的类型统一
     * 
     * 实现说明：
     * 1. 创建一个新的HashMap，大小与输入Map相同
     * 2. 遍历输入Map的所有条目
     * 3. 检查每个键是否为String类型
     * 4. 如果是String类型，则添加到新Map
     * 5. 如果不是String类型，抛出ConfigException
     */
    public static Map<String, Object> castToStringObjectMap(Map<?, ?> inputMap) {
        // 创建新的HashMap，预设容量以优化性能
        Map<String, Object> map = new HashMap<>(inputMap.size());
        // 遍历输入Map的所有条目
        for (Map.Entry<?, ?> entry : inputMap.entrySet()) {
            if (entry.getKey() instanceof String) {
                // 如果键是String类型，直接添加到新Map
                String k = (String) entry.getKey();
                map.put(k, entry.getValue());
            } else {
                // 如果键不是String类型，抛出异常
                throw new ConfigException(String.valueOf(entry.getKey()), entry.getValue(), "Key must be a string.");
            }
        }
        return map;
    }

    /**
     * 将时间戳字符串转换为纪元值（从1970年1月1日00:00:00 GMT开始的毫秒数）
     * 
     * @param timestamp 要转换的时间戳字符串，支持以下格式：
     *                 (1) yyyy-MM-dd'T'HH:mm:ss.SSS，例如：2020-11-10T16:51:38.198
     *                 (2) yyyy-MM-dd'T'HH:mm:ss.SSSZ，例如：2020-11-10T16:51:38.198+0800
     *                 (3) yyyy-MM-dd'T'HH:mm:ss.SSSX，例如：2020-11-10T16:51:38.198+08
     *                 (4) yyyy-MM-dd'T'HH:mm:ss.SSSXX，例如：2020-11-10T16:51:38.198+0800
     *                 (5) yyyy-MM-dd'T'HH:mm:ss.SSSXXX，例如：2020-11-10T16:51:38.198+08:00
     * 
     * @return 时间戳对应的纪元值（毫秒）
     * @throws ParseException 当时间戳不符合ISO8601格式或格式不被支持时
     * @throws IllegalArgumentException 当时间戳为null时
     * 
     * 应用场景：
     * 1. 日志时间戳的标准化处理
     * 2. 消息时间戳的解析
     * 3. 时间序列数据的处理
     * 
     * 实现说明：
     * 1. 首先检查输入是否为null
     * 2. 检查时间戳是否包含ISO8601要求的'T'分隔符
     * 3. 如果时间戳没有时区信息，添加'Z'表示UTC
     * 4. 尝试使用不同的日期格式模式解析时间戳
     * 5. 返回解析后的毫秒时间戳
     */
    public static long getDateTime(String timestamp) throws ParseException, IllegalArgumentException {
        // 检查输入是否为null
        if (timestamp == null) {
            throw new IllegalArgumentException("Error parsing timestamp with null value");
        }

        // 按'T'分割时间戳，检查格式
        final String[] timestampParts = timestamp.split("T");
        if (timestampParts.length < 2) {
            throw new ParseException("Error parsing timestamp. It does not contain a 'T' according to ISO8601 format", timestamp.length());
        }

        // 检查并处理时区信息
        final String secondPart = timestampParts[1];
        if (!(secondPart.contains("+") || secondPart.contains("-") || secondPart.contains("Z"))) {
            // 如果没有时区信息，添加'Z'表示UTC
            timestamp = timestamp + "Z";
        }

        // 创建日期格式化对象
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
        // 设置严格解析模式
        simpleDateFormat.setLenient(false);
        try {
            // 首先尝试使用完整的时区格式（如+08:00）
            simpleDateFormat.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            final Date date = simpleDateFormat.parse(timestamp);
            return date.getTime();
        } catch (final ParseException e) {
            // 如果失败，尝试使用简短的时区格式（如+08）
            simpleDateFormat.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
            final Date date = simpleDateFormat.parse(timestamp);
            return date.getTime();
        }
    }

    /**
     * 检查字符串是否为空（null、空字符串或仅包含空白字符）
     * 
     * @param str 要检查的字符串
     * @return 如果字符串为null、空或仅包含空白字符则返回true；否则返回false
     * 
     * 应用场景：
     * 1. 输入验证
     * 2. 配置项检查
     * 3. 数据清理
     * 
     * 实现说明：
     * 1. 首先检查是否为null
     * 2. 如果不为null，则去除首尾空白字符后检查是否为空字符串
     */
    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * 获取枚举类型所有值的字符串表示数组
     * 
     * @param enumClass 枚举类的Class对象，不能为null
     * @return 包含枚举所有值的字符串表示的数组，永不为null，但可能为空数组
     * 
     * 应用场景：
     * 1. 配置选项的验证
     * 2. 枚举值的序列化
     * 3. 用户界面选项的生成
     * 
     * 实现说明：
     * 1. 检查输入类是否为null
     * 2. 验证输入类是否为枚举类型
     * 3. 使用Stream API获取所有枚举常量
     * 4. 将每个枚举常量转换为字符串
     * 5. 收集结果到字符串数组
     */
    public static String[] enumOptions(Class<? extends Enum<?>> enumClass) {
        // 检查输入是否为null
        Objects.requireNonNull(enumClass);
        // 验证是否为枚举类型
        if (!enumClass.isEnum()) {
            throw new IllegalArgumentException("Class " + enumClass + " is not an enumerable type");
        }

        // 使用Stream API处理枚举常量
        return Stream.of(enumClass.getEnumConstants())
                .map(Object::toString)
                .toArray(String[]::new);
    }

    /**
     * 确保类是具体的（非抽象的）并且是指定基类的子类
     * 如果类是抽象的或不是指定基类的子类，则抛出ConfigException异常
     * 异常消息会友好地提示可用的具体子类（如果有的话）
     * 
     * @param baseClass 期望的父类，不能为null
     * @param klass 要检查的类，不能为null
     * @throws ConfigException 如果类不是具体的或不是指定基类的子类
     * 
     * 应用场景：
     * 1. 插件系统的类型检查
     * 2. 工厂方法的类型验证
     * 3. 配置系统的类加载验证
     * 
     * 实现说明：
     * 1. 检查输入参数是否为null
     * 2. 验证继承关系
     * 3. 检查类是否为抽象类
     * 4. 如果是抽象类，收集并提示可用的具体实现类
     */
    public static void ensureConcreteSubclass(Class<?> baseClass, Class<?> klass) {
        // 检查输入参数
        Objects.requireNonNull(baseClass);
        Objects.requireNonNull(klass);

        // 检查继承关系
        if (!baseClass.isAssignableFrom(klass)) {
            // 确定适当的错误消息用词
            String inheritFrom = baseClass.isInterface() ? "implement" : "extend";
            String baseClassType = baseClass.isInterface() ? "interface" : "class";
            throw new ConfigException("Class " + klass + " does not " + inheritFrom + " the " + baseClass.getSimpleName() + " " + baseClassType);
        }

        // 检查是否为抽象类
        if (Modifier.isAbstract(klass.getModifiers())) {
            // 收集所有可用的具体子类
            String childClassNames = Stream.of(klass.getClasses())
                    .filter(baseClass::isAssignableFrom)  // 筛选继承自基类的类
                    .filter(c -> !Modifier.isAbstract(c.getModifiers()))  // 筛选非抽象类
                    .filter(c -> Modifier.isPublic(c.getModifiers()))  // 筛选公共类
                    .map(Class::getName)  // 获取类名
                    .collect(Collectors.joining(", "));  // 用逗号连接
            
            // 构建错误消息
            String message = "This class is abstract and cannot be created.";
            if (!Utils.isBlank(childClassNames))
                message += " Did you mean " + childClassNames + "?";
            throw new ConfigException(message);
        }
    }

    /**
     * 将时间戳转换为可读的日志格式字符串
     * 
     * @param timestamp 要转换的时间戳
     * @return 格式化后的时间字符串，格式为"yyyy-MM-dd HH:mm:ss,SSS XXX"
     * 
     * 应用场景：
     * 1. 日志记录时的时间戳格式化
     * 2. 事件时间的人类可读展示
     * 3. 调试和监控输出
     * 
     * 实现说明：
     * 1. 创建DateTimeFormatter实例，指定输出格式
     * 2. 将时间戳转换为Instant对象
     * 3. 使用系统默认时区创建ZonedDateTime
     * 4. 应用格式化模式输出字符串
     */
    public static String toLogDateTimeFormat(long timestamp) {
        // 创建日期时间格式化器，指定输出格式包含日期、时间、毫秒和时区偏移
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS XXX");
        // 将时间戳转换为带时区的日期时间，并按指定格式输出
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).format(dateTimeFormatter);
    }

    /**
     * 替换字符串的后缀
     * 
     * @param str 要处理的原始字符串
     * @param oldSuffix 要替换的旧后缀
     * @param newSuffix 要替换成的新后缀
     * @return 替换后缀后的新字符串
     * @throws IllegalArgumentException 如果原始字符串不是以指定的旧后缀结尾
     * 
     * 应用场景：
     * 1. 文件扩展名的替换
     * 2. URL后缀的修改
     * 3. 字符串模式的转换
     * 
     * 实现说明：
     * 1. 首先验证原始字符串是否以旧后缀结尾
     * 2. 如果验证失败，抛出异常
     * 3. 截取除旧后缀外的部分，并拼接新后缀
     */
    public static String replaceSuffix(String str, String oldSuffix, String newSuffix) {
        // 检查字符串是否以指定后缀结尾
        if (!str.endsWith(oldSuffix))
            throw new IllegalArgumentException("Expected string to end with " + oldSuffix + " but string is " + str);
        // 移除旧后缀并添加新后缀
        return str.substring(0, str.length() - oldSuffix.length()) + newSuffix;
    }

    /**
     * 查找Map中所有以指定前缀开头的键值对，并从结果键中移除该前缀
     * 
     * @param map 要过滤的Map
     * @param prefix 要搜索的前缀
     * @return 一个新的Map，包含所有匹配前缀的键值对，且键已去除前缀
     * @param <V> Map中值的类型
     * 
     * 应用场景：
     * 1. 配置项的分组过滤
     * 2. 命名空间的处理
     * 3. 层级结构数据的提取
     * 
     * 实现说明：
     * 调用三参数版本的方法，默认移除前缀
     */
    public static <V> Map<String, V> entriesWithPrefix(Map<String, V> map, String prefix) {
        // 调用完整版本的方法，设置strip为true表示移除前缀
        return entriesWithPrefix(map, prefix, true);
    }

    /**
     * 查找Map中所有以指定前缀开头的键值对，可选是否保留前缀
     * 
     * @param map 要过滤的Map
     * @param prefix 要搜索的前缀
     * @param strip 是否从结果键中移除前缀
     * @return 一个新的Map，包含所有匹配前缀的键值对
     * @param <V> Map中值的类型
     * 
     * 应用场景：
     * 1. 配置项的条件过滤
     * 2. 数据分组和重组
     * 3. 前缀索引的实现
     * 
     * 实现说明：
     * 调用四参数版本的方法，默认不允许完全匹配
     */
    public static <V> Map<String, V> entriesWithPrefix(Map<String, V> map, String prefix, boolean strip) {
        // 调用完整版本的方法，设置allowMatchingLength为false
        return entriesWithPrefix(map, prefix, strip, false);
    }

    /**
     * 查找Map中所有以指定前缀开头的键值对，提供完整的控制选项
     * 
     * @param map 要过滤的Map
     * @param prefix 要搜索的前缀
     * @param strip 是否从结果键中移除前缀
     * @param allowMatchingLength 是否包含与前缀长度相同的键
     * @return 一个新的Map，包含所有匹配前缀的键值对
     * @param <V> Map中值的类型
     * 
     * 应用场景：
     * 1. 高度自定义的配置过滤
     * 2. 复杂的数据分组场景
     * 3. 精确的前缀匹配需求
     * 
     * 实现说明：
     * 1. 创建结果Map
     * 2. 遍历源Map的所有条目
     * 3. 根据前缀和长度条件筛选
     * 4. 根据strip参数决定是否移除前缀
     */
    public static <V> Map<String, V> entriesWithPrefix(Map<String, V> map, String prefix, boolean strip, boolean allowMatchingLength) {
        // 创建结果集
        Map<String, V> result = new HashMap<>();
        // 遍历原始Map的所有条目
        for (Map.Entry<String, V> entry : map.entrySet()) {
            // 检查键是否以前缀开头，并根据allowMatchingLength参数检查长度条件
            if (entry.getKey().startsWith(prefix) && (allowMatchingLength || entry.getKey().length() > prefix.length())) {
                if (strip)
                    // 如果需要移除前缀，则截取前缀后的部分作为新键
                    result.put(entry.getKey().substring(prefix.length()), entry.getValue());
                else
                    // 如果保留前缀，则使用原始键
                    result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * 检查条件是否满足，如果不满足则抛出异常
     * 
     * @param requirement 要检查的条件
     * @throws IllegalArgumentException 当条件不满足时抛出
     * 
     * 应用场景：
     * 1. 参数有效性验证
     * 2. 状态检查
     * 3. 前置条件验证
     * 
     * 实现说明：
     * 如果条件为false，抛出包含默认错误消息的异常
     */
    public static void require(boolean requirement) {
        // 当条件不满足时抛出异常
        if (!requirement)
            throw new IllegalArgumentException("requirement failed");
    }

    /**
     * 检查条件是否满足，如果不满足则抛出带有自定义错误消息的异常
     * 
     * @param requirement 要检查的条件
     * @param errorMessage 自定义错误消息
     * @throws IllegalArgumentException 当条件不满足时抛出，包含自定义错误消息
     * 
     * 应用场景：
     * 1. 带详细说明的参数验证
     * 2. 自定义错误提示的状态检查
     * 3. 业务规则验证
     * 
     * 实现说明：
     * 如果条件为false，抛出包含指定错误消息的异常
     */
    public static void require(boolean requirement, String errorMessage) {
        // 当条件不满足时抛出带有自定义错误消息的异常
        if (!requirement)
            throw new IllegalArgumentException(errorMessage);
    }

    /**
     * 合并多个配置定义为一个
     * 
     * @param configDefs 要合并的配置定义列表
     * @return 合并后的配置定义
     * 
     * 应用场景：
     * 1. 多个组件的配置整合
     * 2. 配置模板的组合
     * 3. 动态配置的构建
     * 
     * 实现说明：
     * 1. 创建新的配置定义对象
     * 2. 遍历所有输入的配置定义
     * 3. 将每个配置定义的所有键值对添加到结果中
     */
    public static ConfigDef mergeConfigs(List<ConfigDef> configDefs) {
        // 创建新的配置定义对象
        ConfigDef all = new ConfigDef();
        // 遍历并合并所有配置定义
        configDefs.forEach(configDef -> configDef.configKeys().values().forEach(all::define));
        return all;
    }

    /**
     * 可以抛出受检异常的Runnable接口
     * 
     * 应用场景：
     * 1. 异常处理的统一封装
     * 2. 需要抛出受检异常的任务执行
     * 3. 函数式接口的异常处理
     */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
