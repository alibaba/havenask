/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <string>

#ifdef JVM_DEBUG
#define JVM_START_OPTS                                                                                                 \
    " -server -Xrs -Xms4g -Xmx4g -Xss6m -XX:MetaspaceSize=416m -XX:MaxMetaspaceSize=512m "                             \
    "-XX:HeapBaseMinAddress=256g "                                                                                     \
    "-XX:MinMetaspaceFreeRatio=0 "                                                                                     \
    "-XX:MaxMetaspaceFreeRatio=20"                                                                                     \
    " -XX:MaxDirectMemorySize=256m -XX:ReservedCodeCacheSize=512m"                                                     \
    " -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:InitiatingHeapOccupancyPercent=45 "                                    \
    "-XX:G1ReservePercent=10"                                                                                          \
    " -XX:ConcGCThreads=4 -XX:ParallelGCThreads=8 -XX:+ExplicitGCInvokesConcurrent"                                    \
    " -XX:+TieredCompilation -Xloggc:./logs/gc.log"                                                                    \
    " -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+DisableExplicitGC -XX:+UseAsyncGCLog"                            \
    " -Dsun.rmi.dgc.server.gcInterval=2592000000 -Dsun.rmi.dgc.client.gcInterval=2592000000"                           \
    " -XX:+MetaspaceDumpOnOutOfMemoryError -XX:MetaspaceDumpPath=./logs/java.mprof"                                    \
    " -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./logs/java.hprof"                                              \
    " -Dio.netty.noPreferDirect=true -Dio.netty.allocator.type=unpooled"                                               \
    " -Dio.netty.noUnsafe=true -Dio.netty.allocator.maxOrder=8"                                                        \
    " -Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=30000"                          \
    " -XX:-UseBiasedLocking -Dfile.encoding=UTF-8 -Djava.awt.headless=true"                                            \
    " -Djava.security.egd=file:/dev/./urandom -Djdk.tls.ephemeralDHKeySize=2048"                                       \
    " -XX:+AlwaysCompileLoopMethods -XX:-DontCompileHugeMethod"                                                        \
    " -XX:+PrintCompilation -XX:+PrintDeoptReason -verbose:gc"                                                         \
    " -XX:+UnlockDiagnosticVMOptions -XX:+MetaspaceDumpOnOutOfMemoryError "                                            \
    "-XX:+MetaspaceDumpBeforeFullGC "                                                                                  \
    "-XX:+MetaspaceDumpAfterFullGC"                                                                                    \
    " -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime "                                       \
    "-XX:+PrintAdaptiveSizePolicy "                                                                                    \
    "-XX:+PrintTenuringDistribution -XX:+PrintReferenceGC -XX:+G1SummarizeConcMark"
#else
#define JVM_START_OPTS                                                                                                 \
    " -server -Xrs -Xms4g -Xmx4g -Xss6m -XX:MetaspaceSize=416m -XX:MaxMetaspaceSize=512m "                             \
    "-XX:HeapBaseMinAddress=256g "                                                                                     \
    "-XX:MinMetaspaceFreeRatio=0 "                                                                                     \
    "-XX:MaxMetaspaceFreeRatio=20"                                                                                     \
    " -XX:MaxDirectMemorySize=256m -XX:ReservedCodeCacheSize=512m"                                                     \
    " -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:InitiatingHeapOccupancyPercent=45 "                                    \
    "-XX:G1ReservePercent=10"                                                                                          \
    " -XX:ConcGCThreads=4 -XX:ParallelGCThreads=8 -XX:+ExplicitGCInvokesConcurrent"                                    \
    " -XX:+TieredCompilation -Xloggc:./logs/gc.log"                                                                    \
    " -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+DisableExplicitGC -XX:+UseAsyncGCLog"                            \
    " -Dsun.rmi.dgc.server.gcInterval=2592000000 -Dsun.rmi.dgc.client.gcInterval=2592000000"                           \
    " -XX:+MetaspaceDumpOnOutOfMemoryError -XX:MetaspaceDumpPath=./logs/java.mprof"                                    \
    " -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./logs/java.hprof"                                              \
    " -Dio.netty.noPreferDirect=true -Dio.netty.allocator.type=unpooled"                                               \
    " -Dio.netty.noUnsafe=true -Dio.netty.allocator.maxOrder=8"                                                        \
    " -Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=30000"                          \
    " -XX:-UseBiasedLocking -Dfile.encoding=UTF-8 -Djava.awt.headless=true"                                            \
    " -Djava.security.egd=file:/dev/./urandom -Djdk.tls.ephemeralDHKeySize=2048"                                       \
    " -XX:+AlwaysCompileLoopMethods -XX:-DontCompileHugeMethod"
#endif

// jvm debug constant
#define JVM_DEBUG_OPTS " -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address="
#define JVM_DEBUG_OPTS_PATTERN "-Xrunjdwp:transport"
#define JVM_DEFAULT_DEBUG_PORT "7001"

#define START_JVM_DEBUG "JVM_DEBUG_FLAG"
#define JVM_DEBUG_PORT "JVM_DEBUG_PORT"
#define HDFSOPTS_KEY "LIBHDFS_OPTS"
#define HDFS_PATH_FOR_INIT "hdfs://hdpet2main/dii_app/"
#define IQUAN_CLIENT_JAR "/aios/sql/iquan/java/iquan_client.jar"
#define FILE_PROTOCOL_PREFIX "file:///"

// input format
#define INPUT_JSON_FORMAT 1
#define INPUT_PROTOBUF_FORMAT 2

// output format
#define OUTPUT_JSON_FORMAT 1
#define OUTPUT_FB_FORMAT 2
