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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.spi.LocationAwareLogger;

/**
 * 该类提供了一种为日志记录器添加公共上下文的方式，用于自动丰富日志消息。
 * 例如，在KafkaConsumer中，通常需要知道消费者的groupId，这个信息可以被添加到上下文对象中，
 * 然后传递给所有依赖组件以构建新的日志记录器。这样就不需要在每条消息中手动添加groupId。
 * 
 * 主要功能：
 * 1. 为日志消息添加统一的前缀
 * 2. 支持不同日志级别（trace, debug, info, warn, error）
 * 3. 支持位置感知和位置无关两种日志记录方式
 * 4. 提供统一的日志格式化和异常处理
 */
public class LogContext {

    /**
     * 日志前缀
     * 用于在每条日志消息前添加统一的标识信息，如groupId、clientId等
     */
    private final String logPrefix;

    /**
     * 创建一个带有指定前缀的日志上下文
     * 
     * @param logPrefix 日志前缀，如果为null则使用空字符串
     */
    public LogContext(String logPrefix) {
        this.logPrefix = logPrefix == null ? "" : logPrefix;
    }

    /**
     * 创建一个没有前缀的日志上下文
     */
    public LogContext() {
        this("");
    }

    /**
     * 为指定类创建一个日志记录器
     * 
     * @param clazz 需要记录日志的类
     * @return 根据底层日志实现返回相应的日志记录器：
     *         - 如果底层支持位置感知，返回LocationAwareKafkaLogger
     *         - 否则返回LocationIgnorantKafkaLogger
     */
    public Logger logger(Class<?> clazz) {
        Logger logger = LoggerFactory.getLogger(clazz);
        if (logger instanceof LocationAwareLogger) {
            return new LocationAwareKafkaLogger(logPrefix, (LocationAwareLogger) logger);
        } else {
            return new LocationIgnorantKafkaLogger(logPrefix, logger);
        }
    }

    public String logPrefix() {
        return logPrefix;
    }

    /**
     * Kafka日志记录器的抽象基类
     * 实现了Logger接口，提供了添加前缀的基本功能
     */
    private abstract static class AbstractKafkaLogger implements Logger {
        private final String prefix;

        protected AbstractKafkaLogger(final String prefix) {
            this.prefix = prefix;
        }

        protected String addPrefix(final String message) {
            return prefix + message;
        }
    }

    /**
     * 位置感知的Kafka日志记录器
     * 能够记录日志产生的具体类名、方法名和行号
     * 通过LocationAwareLogger接口实现更精确的日志记录
     */
    private static class LocationAwareKafkaLogger extends AbstractKafkaLogger {
        private final LocationAwareLogger logger;
        private final String fqcn;

        LocationAwareKafkaLogger(String logPrefix, LocationAwareLogger logger) {
            super(logPrefix);
            this.logger = logger;
            this.fqcn = LocationAwareKafkaLogger.class.getName();
        }

        @Override
        public String getName() {
            return logger.getName();
        }

        @Override
        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override
        public boolean isTraceEnabled(Marker marker) {
            return logger.isTraceEnabled(marker);
        }

        @Override
        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override
        public boolean isDebugEnabled(Marker marker) {
            return logger.isDebugEnabled(marker);
        }

        @Override
        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override
        public boolean isInfoEnabled(Marker marker) {
            return logger.isInfoEnabled(marker);
        }

        @Override
        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override
        public boolean isWarnEnabled(Marker marker) {
            return logger.isWarnEnabled(marker);
        }

        @Override
        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        @Override
        public boolean isErrorEnabled(Marker marker) {
            return logger.isErrorEnabled(marker);
        }

        @Override
        public void trace(String message) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, message, null, null);
            }
        }

        @Override
        public void trace(String format, Object arg) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void trace(String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void trace(String format, Object... args) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, format, args, null);
            }
        }

        @Override
        public void trace(String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, msg, null, t);
            }
        }

        @Override
        public void trace(Marker marker, String msg) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, msg, null, null);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, argArray, null);
            }
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, msg, null, t);
            }
        }

        @Override
        public void debug(String message) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, message, null, null);
            }
        }

        @Override
        public void debug(String format, Object arg) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void debug(String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void debug(String format, Object... args) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, args, null);
            }
        }

        @Override
        public void debug(String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, msg, null, t);
            }
        }

        @Override
        public void debug(Marker marker, String msg) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, msg, null, null);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, arguments, null);
            }
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, msg, null, t);
            }
        }

        @Override
        public void warn(String message) {
            writeLog(null, LocationAwareLogger.WARN_INT, message, null, null);
        }

        @Override
        public void warn(String format, Object arg) {
            writeLog(null, LocationAwareLogger.WARN_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void warn(String message, Object arg1, Object arg2) {
            writeLog(null, LocationAwareLogger.WARN_INT, message, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void warn(String format, Object... args) {
            writeLog(null, LocationAwareLogger.WARN_INT, format, args, null);
        }

        @Override
        public void warn(String msg, Throwable t) {
            writeLog(null, LocationAwareLogger.WARN_INT, msg, null, t);
        }

        @Override
        public void warn(Marker marker, String msg) {
            writeLog(marker, LocationAwareLogger.WARN_INT, msg, null, null);
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            writeLog(marker, LocationAwareLogger.WARN_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            writeLog(marker, LocationAwareLogger.WARN_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            writeLog(marker, LocationAwareLogger.WARN_INT, format, arguments, null);
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            writeLog(marker, LocationAwareLogger.WARN_INT, msg, null, t);
        }

        @Override
        public void error(String message) {
            writeLog(null, LocationAwareLogger.ERROR_INT, message, null, null);
        }

        @Override
        public void error(String format, Object arg) {
            writeLog(null, LocationAwareLogger.ERROR_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void error(String format, Object arg1, Object arg2) {
            writeLog(null, LocationAwareLogger.ERROR_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void error(String format, Object... args) {
            writeLog(null, LocationAwareLogger.ERROR_INT, format, args, null);
        }

        @Override
        public void error(String msg, Throwable t) {
            writeLog(null, LocationAwareLogger.ERROR_INT, msg, null, t);
        }

        @Override
        public void error(Marker marker, String msg) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, msg, null, null);
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, arguments, null);
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, msg, null, t);
        }

        @Override
        public void info(String msg) {
            writeLog(null, LocationAwareLogger.INFO_INT, msg, null, null);
        }

        @Override
        public void info(String format, Object arg) {
            writeLog(null, LocationAwareLogger.INFO_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void info(String format, Object arg1, Object arg2) {
            writeLog(null, LocationAwareLogger.INFO_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void info(String format, Object... args) {
            writeLog(null, LocationAwareLogger.INFO_INT, format, args, null);
        }

        @Override
        public void info(String msg, Throwable t) {
            writeLog(null, LocationAwareLogger.INFO_INT, msg, null, t);
        }

        @Override
        public void info(Marker marker, String msg) {
            writeLog(marker, LocationAwareLogger.INFO_INT, msg, null, null);
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            writeLog(marker, LocationAwareLogger.INFO_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            writeLog(marker, LocationAwareLogger.INFO_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            writeLog(marker, LocationAwareLogger.INFO_INT, format, arguments, null);
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            writeLog(marker, LocationAwareLogger.INFO_INT, msg, null, t);
        }

        /**
         * 写入日志的核心方法
         * 
         * @param marker 日志标记
         * @param level 日志级别
         * @param format 日志格式
         * @param args 格式化参数
         * @param exception 异常信息
         */
        private void writeLog(Marker marker, int level, String format, Object[] args, Throwable exception) {
            String message = format;
            if (args != null && args.length > 0) {
                FormattingTuple formatted = MessageFormatter.arrayFormat(format, args);
                if (exception == null && formatted.getThrowable() != null) {
                    exception = formatted.getThrowable();
                }
                message = formatted.getMessage();
            }
            logger.log(marker, fqcn, level, addPrefix(message), null, exception);
        }
    }

    /**
     * 位置无关的Kafka日志记录器
     * 不记录日志产生的具体位置信息
     * 直接使用标准的Logger接口进行日志记录
     */
    private static class LocationIgnorantKafkaLogger extends AbstractKafkaLogger {
        private final Logger logger;

        LocationIgnorantKafkaLogger(String logPrefix, Logger logger) {
            super(logPrefix);
            this.logger = logger;
        }

        @Override
        public String getName() {
            return logger.getName();
        }

        @Override
        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override
        public boolean isTraceEnabled(Marker marker) {
            return logger.isTraceEnabled(marker);
        }

        @Override
        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override
        public boolean isDebugEnabled(Marker marker) {
            return logger.isDebugEnabled(marker);
        }

        @Override
        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override
        public boolean isInfoEnabled(Marker marker) {
            return logger.isInfoEnabled(marker);
        }

        @Override
        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override
        public boolean isWarnEnabled(Marker marker) {
            return logger.isWarnEnabled(marker);
        }

        @Override
        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        @Override
        public boolean isErrorEnabled(Marker marker) {
            return logger.isErrorEnabled(marker);
        }

        @Override
        public void trace(String message) {
            if (logger.isTraceEnabled()) {
                logger.trace(addPrefix(message));
            }
        }

        @Override
        public void trace(String message, Object arg) {
            if (logger.isTraceEnabled()) {
                logger.trace(addPrefix(message), arg);
            }
        }

        @Override
        public void trace(String message, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                logger.trace(addPrefix(message), arg1, arg2);
            }
        }

        @Override
        public void trace(String message, Object... args) {
            if (logger.isTraceEnabled()) {
                logger.trace(addPrefix(message), args);
            }
        }

        @Override
        public void trace(String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                logger.trace(addPrefix(msg), t);
            }
        }

        @Override
        public void trace(Marker marker, String msg) {
            if (logger.isTraceEnabled()) {
                logger.trace(marker, addPrefix(msg));
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
            if (logger.isTraceEnabled()) {
                logger.trace(marker, addPrefix(format), arg);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                logger.trace(marker, addPrefix(format), arg1, arg2);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray) {
            if (logger.isTraceEnabled()) {
                logger.trace(marker, addPrefix(format), argArray);
            }
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                logger.trace(marker, addPrefix(msg), t);
            }
        }

        @Override
        public void debug(String message) {
            if (logger.isDebugEnabled()) {
                logger.debug(addPrefix(message));
            }
        }

        @Override
        public void debug(String message, Object arg) {
            if (logger.isDebugEnabled()) {
                logger.debug(addPrefix(message), arg);
            }
        }

        @Override
        public void debug(String message, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                logger.debug(addPrefix(message), arg1, arg2);
            }
        }

        @Override
        public void debug(String message, Object... args) {
            if (logger.isDebugEnabled()) {
                logger.debug(addPrefix(message), args);
            }
        }

        @Override
        public void debug(String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug(addPrefix(msg), t);
            }
        }

        @Override
        public void debug(Marker marker, String msg) {
            if (logger.isDebugEnabled()) {
                logger.debug(marker, addPrefix(msg));
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
            if (logger.isDebugEnabled()) {
                logger.debug(marker, addPrefix(format), arg);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                logger.debug(marker, addPrefix(format), arg1, arg2);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
            if (logger.isDebugEnabled()) {
                logger.debug(marker, addPrefix(format), arguments);
            }
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug(marker, addPrefix(msg), t);
            }
        }

        @Override
        public void warn(String message) {
            logger.warn(addPrefix(message));
        }

        @Override
        public void warn(String message, Object arg) {
            logger.warn(addPrefix(message), arg);
        }

        @Override
        public void warn(String message, Object arg1, Object arg2) {
            logger.warn(addPrefix(message), arg1, arg2);
        }

        @Override
        public void warn(String message, Object... args) {
            logger.warn(addPrefix(message), args);
        }

        @Override
        public void warn(String msg, Throwable t) {
            logger.warn(addPrefix(msg), t);
        }

        @Override
        public void warn(Marker marker, String msg) {
            logger.warn(marker, addPrefix(msg));
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            logger.warn(marker, addPrefix(format), arg);
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            logger.warn(marker, addPrefix(format), arg1, arg2);
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            logger.warn(marker, addPrefix(format), arguments);
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            logger.warn(marker, addPrefix(msg), t);
        }

        @Override
        public void error(String message) {
            logger.error(addPrefix(message));
        }

        @Override
        public void error(String message, Object arg) {
            logger.error(addPrefix(message), arg);
        }

        @Override
        public void error(String message, Object arg1, Object arg2) {
            logger.error(addPrefix(message), arg1, arg2);
        }

        @Override
        public void error(String message, Object... args) {
            logger.error(addPrefix(message), args);
        }

        @Override
        public void error(String msg, Throwable t) {
            logger.error(addPrefix(msg), t);
        }

        @Override
        public void error(Marker marker, String msg) {
            logger.error(marker, addPrefix(msg));
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            logger.error(marker, addPrefix(format), arg);
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            logger.error(marker, addPrefix(format), arg1, arg2);
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            logger.error(marker, addPrefix(format), arguments);
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            logger.error(marker, addPrefix(msg), t);
        }

        @Override
        public void info(String message) {
            logger.info(addPrefix(message));
        }

        @Override
        public void info(String message, Object arg) {
            logger.info(addPrefix(message), arg);
        }

        @Override
        public void info(String message, Object arg1, Object arg2) {
            logger.info(addPrefix(message), arg1, arg2);
        }

        @Override
        public void info(String message, Object... args) {
            logger.info(addPrefix(message), args);
        }

        @Override
        public void info(String msg, Throwable t) {
            logger.info(addPrefix(msg), t);
        }

        @Override
        public void info(Marker marker, String msg) {
            logger.info(marker, addPrefix(msg));
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            logger.info(marker, addPrefix(format), arg);
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            logger.info(marker, addPrefix(format), arg1, arg2);
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            logger.info(marker, addPrefix(format), arguments);
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            logger.info(marker, addPrefix(msg), t);
        }

    }

}
