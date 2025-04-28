/*
 * 授权给Apache软件基金会(ASF)的一个或多个贡献者许可协议。
 * 有关版权所有权的其他信息，请参见随本作品分发的NOTICE文件。
 * ASF根据Apache许可证2.0版（以下简称"许可证"）将本文件授权给您；
 * 除非符合许可证，否则您不得使用此文件。
 * 您可以在以下位置获取许可证副本：
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * 除非适用法律要求或书面同意，否则根据许可证分发的软件是基于
 * "按原样"提供的，没有任何明示或暗示的担保或条件。
 * 有关许可证下的特定语言管理权限和限制，请参见许可证。
 */
package org.apache.kafka.common.network;

import java.util.Objects;

/**
 * CipherInformation类用于存储和管理SSL/TLS加密通信的密码套件和协议信息。
 * 在Kafka的安全通信中，该类主要用于：
 * 1. 记录客户端和服务器之间建立的SSL/TLS连接所使用的加密算法（cipher suite）
 * 2. 跟踪所使用的安全协议版本（如TLSv1.2, TLSv1.3等）
 * 3. 提供加密信息的访问接口，用于日志记录、监控和调试
 */
public class CipherInformation {
    /**
     * 用于存储SSL/TLS连接使用的密码套件名称
     * 例如："TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
     */
    private final String cipher;

    /**
     * 用于存储SSL/TLS连接使用的协议版本
     * 例如："TLSv1.2"或"TLSv1.3"
     */
    private final String protocol;

    /**
     * 创建一个新的CipherInformation实例
     * @param cipher SSL/TLS密码套件名称
     * @param protocol SSL/TLS协议版本
     */
    public CipherInformation(String cipher, String protocol) {
        // 如果cipher参数为null或空字符串，则设置为"unknown"，否则使用传入的值
        this.cipher = cipher == null || cipher.isEmpty()  ? "unknown" : cipher;
        // 如果protocol参数为null或空字符串，则设置为"unknown"，否则使用传入的值
        this.protocol = protocol == null || protocol.isEmpty()  ? "unknown" : protocol;
    }

    /**
     * 获取SSL/TLS连接使用的密码套件名称
     * @return 返回密码套件名称，如果未知则返回"unknown"
     */
    public String cipher() {
        return cipher;
    }

    /**
     * 获取SSL/TLS连接使用的协议版本
     * @return 返回协议版本，如果未知则返回"unknown"
     */
    public String protocol() {
        return protocol;
    }

    @Override
    public String toString() {
        return "CipherInformation(cipher=" + cipher +
            ", protocol=" + protocol + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(cipher, protocol);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof CipherInformation)) {
            return false;
        }
        CipherInformation other = (CipherInformation) o;
        return other.cipher.equals(cipher) &&
            other.protocol.equals(protocol);
    }
}
