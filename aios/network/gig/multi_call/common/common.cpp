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
#include "aios/network/gig/multi_call/common/common.h"
#include "google/protobuf/message.h"

namespace multi_call {

#define MULTILINE(...) #__VA_ARGS__

const std::string MC_PROTOCOL_TCP_STR = "tcp";
const std::string MC_PROTOCOL_RAW_ARPC_STR = "raw_arpc";
const std::string MC_PROTOCOL_ARPC_STR = "arpc";
const std::string MC_PROTOCOL_HTTP_STR = "http";
const std::string MC_PROTOCOL_GRPC_STR = "grpc";
const std::string MC_PROTOCOL_GRPC_STREAM_STR = "grpc_stream";
const std::string MC_PROTOCOL_RDMA_ARPC_STR = "rdma_arpc";
const std::string MC_PROTOCOL_UNKNOWN_STR = "unknown";
const std::string MC_SUBSCRIBE_TYPE_CM2_STR = "cm2";
const std::string MC_SUBSCRIBE_TYPE_VIP_STR = "vip";
const std::string MC_SUBSCRIBE_TYPE_ISTIO_STR = "istio";
const std::string MC_SUBSCRIBE_TYPE_LOCAL_STR = "local";
const std::string MC_SUBSCRIBE_TYPE_HEARTBEAT_STR = "heartbeat";

const std::string MULTI_CALL_GROUP_METRIC = "multi_call/";
const std::string EMPTY_STRING = "";
const std::string GIG_CLUSTER_BIZ_SPLITTER = ".";

const std::string GIG_QUERY_CHECKSUM = "GigQuery";
const std::string GIG_RESPONSE_CHECKSUM = "GigResponse";
const std::string GIG_DATA = "GigData";
// just for compatible
const std::string GIG_DATA_C = "gig_data";

const std::string GIG_GRPC_REQUEST_INFO_KEY = "gig_grpc_request_info_key-bin";
const std::string GIG_GRPC_RESPONSE_INFO_KEY = "gig_grpc_response_info_key-bin";
const std::string GIG_GRPC_TYPE_KEY = "gig_grpc_type_key";
const std::string GIG_GRPC_TYPE_STREAM = "stream";
const std::string GIG_GRPC_HANDLER_ID_KEY = "gig_grpc_handler_id_key";

const std::string GIG_GRPC_HEARTBEAT_METHOD_NAME = "gig_heartbeat_grpc";

const std::string GIG_ARPC_HEARTBEAT_METHOD_NAME = "gig_heartbeat";

const std::string GIG_UNIFY_CALL_METHOD_NAME = "process";

const std::string GIG_HEARTBEAT_METHOD_NAME = "gig_heartbeat";
const std::string GIG_NEW_HEARTBEAT_METHOD_NAME = "gig.heartbeat";

const std::string CM2_SERVICE_META_KEY = "carbon_meta";
const std::string CM2_SERVICE_META_CLUSTER = "HIPPO_CLUSTER";
const std::string CM2_SERVICE_META_APP = "HIPPO_APP";
const std::string CM2_SERVICE_META_ROLE = "C2_ROLE";
const std::string CM2_SERVICE_META_GROUP = "C2_GROUP";
const std::string CM2_SERVICE_META_PLATFORM = "PLATFORM";

const std::string ENV_META_CLUSTER = "HIPPO_SERVICE_NAME";
const std::string ENV_META_APP = "HIPPO_APP";
const std::string ENV_META_ROLE = "C2_ROLE";
const std::string ENV_META_GROUP = "C2_GROUP";
const std::string ENV_META_PLATFORM = "PLATFORM";

const std::string GIG_UNKNOWN = "unknown";
const std::string GIG_TAG_PREFIX = "gig_";
const std::string GIG_TAG_SRC = GIG_TAG_PREFIX + "src";
const std::string GIG_TAG_SRC_AB = GIG_TAG_PREFIX + "src_ab";

const std::string RPC_DATA_SRC = GIG_TAG_SRC;
const std::string RPC_DATA_SRC_AB = GIG_TAG_SRC_AB;
const std::string RPC_DATA_STRESS_TEST = "t";

const std::string GIG_TAG_STRESS_TEST = GIG_TAG_PREFIX + RPC_DATA_STRESS_TEST;
const std::string GIG_TAG_PLATFORM = GIG_TAG_PREFIX + "platform";
const std::string GIG_TAG_GROUP = "c2_group";
const std::string GIG_TAG_ROLE = "c2_role";
const std::string GIG_TAG_BIZ = GIG_TAG_PREFIX + "biz";
const std::string GIG_TAG_TARGET_PREFIX = GIG_TAG_PREFIX + "target_";
const std::string GIG_TAG_TARGET_CLUSTER =
    GIG_TAG_TARGET_PREFIX + "hippo_cluster";
const std::string GIG_TAG_TARGET_PLATFORM = GIG_TAG_TARGET_PREFIX + "platform";
const std::string GIG_TAG_TARGET_GROUP = GIG_TAG_TARGET_PREFIX + "c2_group";
const std::string GIG_TAG_TARGET_ROLE = GIG_TAG_TARGET_PREFIX + "c2_role";
const std::string GIG_TAG_TARGET_APP = GIG_TAG_TARGET_PREFIX + "hippo_app";
const std::string GIG_TAG_TARGET_BIZ = GIG_TAG_TARGET_PREFIX + "biz";
const std::string GIG_TAG_RESERVED_0 = GIG_TAG_PREFIX + "reserved_0";
const std::string GIG_TAG_RESERVED_1 = GIG_TAG_PREFIX + "reserved_1";
const std::string GIG_KMONITOR = "gig_kmonitor";

const std::string GIG_RDMA_PORT_DIFF_KEY = "cm2RdmaPortDiff";

const std::string DEFAULT_KMONITOR_CONFIG = MULTILINE({
    "tenant_name" : "default",
    "service_name" : "kmon",
    "sink_address" : "localhost:4141",
    "global_tags" : {}
});

const std::string LOAD_BALANCE_VERSION_GROUP_BIZS_KEY =
    "load_banlance_version_group_bizs_key";

ProtocolType convertProtocolType(const std::string &typeStr) {
    if (MC_PROTOCOL_HTTP_STR == typeStr) {
        return MC_PROTOCOL_HTTP;
    } else if (MC_PROTOCOL_TCP_STR == typeStr) {
        return MC_PROTOCOL_TCP;
    } else if (MC_PROTOCOL_RAW_ARPC_STR == typeStr) {
        return MC_PROTOCOL_ARPC;
    } else if (MC_PROTOCOL_ARPC_STR == typeStr) {
        return MC_PROTOCOL_ARPC;
    } else if (MC_PROTOCOL_GRPC_STR == typeStr) {
        return MC_PROTOCOL_GRPC;
    } else if (MC_PROTOCOL_GRPC_STREAM_STR == typeStr) {
        return MC_PROTOCOL_GRPC_STREAM;
    } else if (MC_PROTOCOL_RDMA_ARPC_STR == typeStr) {
        return MC_PROTOCOL_RDMA_ARPC;
    } else {
        return MC_PROTOCOL_UNKNOWN;
    }
}

const std::string &convertProtocolTypeToStr(ProtocolType type) {
    static std::string typeStrArr[] = {
        MC_PROTOCOL_TCP_STR, MC_PROTOCOL_ARPC_STR, MC_PROTOCOL_HTTP_STR,
        MC_PROTOCOL_GRPC_STR, MC_PROTOCOL_GRPC_STREAM_STR, MC_PROTOCOL_RDMA_ARPC_STR,
        MC_PROTOCOL_UNKNOWN_STR
    };
    return typeStrArr[type];
}

const std::string &convertRequestTypeToStr(RequestType type) {
    static std::string requestTypeStrArr[] = {
        "RT_NORMAL", "RT_PROBE", "RT_COPY"
    };
    return requestTypeStrArr[type];
}

const std::string &convertSubscribeTypeToStr(SubscribeType type) {
    switch (type) {
    case ST_CM2:
        return MC_SUBSCRIBE_TYPE_CM2_STR;
    case ST_VIP:
        return MC_SUBSCRIBE_TYPE_VIP_STR;
    case ST_ISTIO:
        return MC_SUBSCRIBE_TYPE_ISTIO_STR;
    case ST_LOCAL:
        return MC_SUBSCRIBE_TYPE_LOCAL_STR;
    case ST_HEARTBEAT:
        return MC_SUBSCRIBE_TYPE_HEARTBEAT_STR;
    default:
        return GIG_UNKNOWN;
    }
}

void freeProtoMessage(google::protobuf::Message *message) {
    if (unlikely(message && !message->GetArena())) {
        delete message;
    }
}

std::string getTimeStrFromUs(int64_t timeUs) {
    time_t sec = timeUs / 1000000LL;
    int64_t usec = timeUs % 1000000LL;
    struct tm tim;
    ::localtime_r(&sec, &tim);

    constexpr int32_t timeLen = 48;
    char buffer[timeLen];
    snprintf(buffer, timeLen, "%04d-%02d-%02d %02d:%02d:%02d.%06ld", tim.tm_year + 1900,
             tim.tm_mon + 1, tim.tm_mday, tim.tm_hour, tim.tm_min, tim.tm_sec, usec);
    return std::string(buffer);
}

} // namespace multi_call
