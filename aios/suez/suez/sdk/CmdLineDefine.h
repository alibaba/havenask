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

namespace suez {

static const std::string HIPPO_SLAVE_IP("HIPPO_SLAVE_IP");
static const std::string HIPPO_DP2_SLAVE_PORT("HIPPO_DP2_SLAVE_PORT");
static const std::string HIPPO_PROC_INSTANCEID("HIPPO_PROC_INSTANCEID");
static const std::string CARBON_PACKAGE_CHECKSUM("packageCheckSum");

static const std::string HTTP_PORT("httpPort");
static const std::string AMONITOR_PORT("amonitorPort");
static const std::string AMONITOR_PATH("amonitorPath");
static const std::string AMONITOR_PATH_STYLE("amonitorPathStyle");
static const std::string SERVICE_NAME("serviceName");
static const std::string HTTP_THREAD_NUM("httpThreadNum");
static const std::string HTTP_QUEUE_SIZE("httpQueueSize");
static const std::string WAIT_FOR_DEBUGGER("waitForDebugger");
static const std::string DECODE_URI("decodeUri");
static const std::string HA_COMPATABLE("haCompatible");
static const std::string ROLE_TYPE("roleType");
static const std::string ROLE_NAME("roleName");
static const std::string ZONE_NAME("zoneName");
static const std::string PART_ID("partId");
static const std::string HTTP_IO_THREAD_NUM("httpIOThreadNum");
static const std::string DP_THREAD_NUM("dpThreadNum");
static const std::string LOAD_THREAD_NUM("loadThreadNum");
static const std::string DECISION_LOOP_INTERVAL("decisionLoopInterval");
static const std::string REPORT_INDEX_STATUS_LOOP_INTERVAL("reportIndexStatusInterval");
static const std::string WORKER_IDENTIFIER_FOR_CARBON("WORKER_IDENTIFIER_FOR_CARBON");
static const std::string ALLOW_PARTIAL_TABLE_READY("allowPartialTableReady");
static const std::string NEED_SHUTDOWN_GRACEFULLY("needShutdownGracefully");
static const std::string NO_DISK_QUOTA_CHECK("noDiskQuotaCheck");
static const std::string DEBUG_MODE("debugMode");
static const std::string LOCAL_MODE("localMode");
static const std::string DISCARD_METRIC_SAMPLE_INTERVAL("discardMetricSampleInterval");
static const std::string LEADER_ELECTION_STRATEGY_TYPE("leader_election_strategy_type");

// upper case format string
static const std::string RS_ALLOW_RELOAD_BY_CONFIG("RS_ALLOW_RELOAD_BY_CONFIG");
static const std::string RS_ALLOW_RELOAD_BY_INDEX_ROOT("RS_ALLOW_RELOAD_BY_INDEX_ROOT");
static const std::string RS_ALLOW_FORCELOAD("RS_ALLOW_FORCELOAD");
static const std::string RS_ALLOW_MEMORY("RS_ALLOW_MEMORY");
static const std::string RS_ALLOW_RATIO("RS_ALLOW_RATIO");
static const std::string RS_ALLOW_BY_CONFIG("RS_ALLOW_BY_CONFIG");

// for kmon client
static const std::string KMONITOR_PORT("kmonitorPort");
static const std::string KMONITOR_SINK_ADDRESS("kmonitorSinkAddress");
static const std::string KMONITOR_ENABLE_LOGFILE_SINK("kmonitorEnableLogFileSink");
static const std::string KMONITOR_SERVICE_NAME("kmonitorServiceName");
static const std::string KMONITOR_TENANT("kmonitorTenant");
static const std::string KMONITOR_METRICS_PREFIX("kmonitorMetricsPrefix");
static const std::string KMONITOR_GLOBAL_TABLE_METRICS_PREFIX("kmonitorGlobalTableMetricsPrefix");
static const std::string KMONITOR_TABLE_METRICS_PREFIX("kmonitorTableMetricsPrefix");
static const std::string KMONITOR_METRICS_REPORTER_CACHE_LIMIT("kmonitorMetricsReporterCacehLimit");
static const std::string KMONITOR_TAGS("kmonitorTags");
static const std::string KMONITOR_NORMAL_SAMPLE_PERIOD("kmonitorNormalSamplePeriod");

// for rdma arpc
static const std::string RDMA_PORT("rdmaPort");
static const std::string RDMA_RPC_THREAD_NUM("rdmaRpcThreadNum");
static const std::string RDMA_RPC_QUEUE_SIZE("rdmaRpcQueueSize");
static const std::string RDMA_WORKER_THREAD_NUM("rdmaWorkerThreadNum");
static const std::string RDMA_IO_THREAD_NUM("rdmaIoThreadNum");

// for grpc
static const std::string GIG_GRPC_PORT("gigGrpcPort");
static const std::string GIG_GRPC_THREAD_NUM("gigGrpcThreadNum");
static const std::string GIG_GRPC_CERTS_DIR("gigGrpcCertsDir");
static const std::string GIG_GRPC_TARGET_NAME("gigGrpcTargetName");
static const std::string GIG_ENABLE_AGENT_STAT("gigEnableAgentStat");

} // namespace suez
