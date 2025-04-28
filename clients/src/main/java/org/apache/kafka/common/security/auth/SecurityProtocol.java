/*
 * 授权给Apache软件基金会(ASF)的许可证，遵循Apache许可证2.0版本
 * 详细信息请参阅LICENSE文件
 */
package org.apache.kafka.common.security.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Kafka安全协议枚举类
 * 
 * 该枚举定义了Kafka支持的所有安全协议类型，包括：
 * 1. 无认证无加密的PLAINTEXT
 * 2. 基于SSL/TLS的加密传输
 * 3. 基于SASL的认证但不加密传输
 * 4. 同时使用SASL认证和SSL加密的安全传输
 * 
 * 每个协议类型都有唯一的ID和名称，用于在配置和内部通信中标识不同的安全协议。
 */
public enum SecurityProtocol {
    /** 无认证无加密的通道，适用于开发测试环境或安全的内部网络 */
    PLAINTEXT(0, "PLAINTEXT"),
    /** SSL加密通道，提供传输层安全性，支持双向认证 */
    SSL(1, "SSL"),
    /** SASL认证但不加密的通道，适用于需要身份认证但网络已加密的场景 */
    SASL_PLAINTEXT(2, "SASL_PLAINTEXT"),
    /** SASL认证且使用SSL加密的通道，提供最高级别的安全性 */
    SASL_SSL(3, "SASL_SSL");

    /** 协议ID到安全协议实例的映射，用于快速查找 */
    private static final Map<Short, SecurityProtocol> CODE_TO_SECURITY_PROTOCOL;
    /** 所有安全协议名称的列表 */
    private static final List<String> NAMES;

    static {
        // 初始化协议映射和名称列表
        SecurityProtocol[] protocols = SecurityProtocol.values();
        List<String> names = new ArrayList<>(protocols.length);
        Map<Short, SecurityProtocol> codeToSecurityProtocol = new HashMap<>(protocols.length);
        
        // 遍历所有协议类型，建立ID映射和名称列表
        for (SecurityProtocol proto : protocols) {
            codeToSecurityProtocol.put(proto.id, proto);
            names.add(proto.name);
        }
        
        // 使用不可修改的集合包装，确保线程安全性
        CODE_TO_SECURITY_PROTOCOL = Collections.unmodifiableMap(codeToSecurityProtocol);
        NAMES = Collections.unmodifiableList(names);
    }

    /** 
     * 安全协议的永久且不可变的ID
     * 该ID必须与kafka.cluster.SecurityProtocol中的定义匹配
     * 用于在网络传输和存储中标识协议类型
     */
    public final short id;

    /** 
     * 安全协议的名称
     * 用于客户端配置和日志记录
     * 在配置文件中使用此名称指定安全协议
     */
    public final String name;

    /**
     * 构造函数
     * @param id 协议ID
     * @param name 协议名称
     */
    SecurityProtocol(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

    /**
     * 获取所有安全协议的名称列表
     * @return 不可修改的协议名称列表
     */
    public static List<String> names() {
        return NAMES;
    }

    /**
     * 根据协议ID查找对应的安全协议
     * @param id 要查找的协议ID
     * @return 对应的SecurityProtocol实例，如果未找到则返回null
     */
    public static SecurityProtocol forId(short id) {
        return CODE_TO_SECURITY_PROTOCOL.get(id);
    }

    /**
     * 根据协议名称查找对应的安全协议（大小写不敏感）
     * @param name 要查找的协议名称
     * @return 对应的SecurityProtocol实例
     * @throws IllegalArgumentException 如果找不到对应的协议
     */
    public static SecurityProtocol forName(String name) {
        return SecurityProtocol.valueOf(name.toUpperCase(Locale.ROOT));
    }

}
