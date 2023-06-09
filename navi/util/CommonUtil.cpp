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
#include "navi/util/CommonUtil.h"
#include "autil/TimeUtility.h"
#include "autil/StringUtil.h"
#include <multi_call/util/RandomGenerator.h>

namespace navi {

CommonUtil::CommonUtil() {
}

CommonUtil::~CommonUtil() {
}

std::string CommonUtil::getPortKey(const std::string &node,
                                   const std::string &port)
{
    if (port.empty()) {
        return node;
    }
    return node + NODE_PORT_SEPERATOR + port;
}

bool CommonUtil::parseGroupPort(const std::string &port, std::string &name,
                                IndexType &index)
{
    std::vector<std::string> vec;
    autil::StringUtil::split(vec, port, GROUP_PORT_SEPERATOR);
    auto size = vec.size();
    if (0u == size || size > 2u) {
        return false;
    }
    name = vec[0];
    if (2u == size) {
        if (!autil::StringUtil::fromString(vec[1], index)) {
            index = INVALID_INDEX;
            return false;
        }
    } else {
        index = 0;
    }
    return true;
}

std::string CommonUtil::getGroupPortName(const std::string &port, IndexType index) {
    return port + GROUP_PORT_SEPERATOR + std::to_string(index);
}

std::string CommonUtil::getGraphPortName(const std::string &node,
                                         const std::string &port)
{
    if (port.empty()) {
        return node;
    }
    return CommonUtil::getPortKey(node, port);
}

const char *CommonUtil::getErrorString(ErrorCode ec) {
    switch (ec) {
    case EC_NONE:
        return "EC_NONE";
    case EC_ABORT:
        return "EC_ABORT";
    case EC_DEADLOCK:
        return "EC_DEADLOCK";
    case EC_INVALID_ATTRIBUTE:
        return "EC_INVALID_ATTRIBUTE";
    case EC_INIT_GRAPH:
        return "EC_INIT_GRAPH";
    case EC_INIT_SUB_GRAPH:
        return "EC_INIT_SUB_GRAPH";
    case EC_WEAVE_GRAPH:
        return "EC_WEAVE_GRAPH";
    case EC_QUEUE_FULL:
        return "EC_QUEUE_FULL";
    case EC_FORK:
        return "EC_FORK";
    case EC_TIMEOUT:
        return "EC_TIMEOUT";
    case EC_INIT_RESOURCE:
        return "EC_INIT_RESOURCE";
    case EC_CREATE_KERNEL:
        return "EC_CREATE_KERNEL";
    case EC_LACK_RESOURCE:
        return "EC_LACK_RESOURCE";
    case EC_RPC_INPUT_PORT:
        return "EC_RPC_INPUT_PORT";
    case EC_PROTO_DEF:
        return "EC_PROTO_DEF";
    case EC_DESERIALIZE:
        return "EC_DESERIALIZE";
    case EC_SERIALIZE:
        return "EC_SERIALIZE";
    case EC_NO_DATA:
        return "EC_NO_DATA";
    case EC_DATA_OVERRIDE:
        return "EC_DATA_OVERRIDE";
    case EC_DATA_AFTER_EOF:
        return "EC_DATA_AFTER_EOF";
    case EC_BUFFER_FULL:
        return "EC_BUFFER_FULL";
    case EC_STREAM_RPC:
        return "EC_STREAM_RPC";
    case EC_STREAM_RECEIVE:
        return "EC_STREAM_RECEIVE";
    case EC_STREAM_SEND:
        return "EC_STREAM_SEND";
    case EC_NO_GRAPH_OUTPUT:
        return "EC_NO_GRAPH_OUTPUT";
    case EC_OVERRIDE_EDGE_DATA:
        return "EC_OVERRIDE_EDGE_DATA";
    case EC_KERNEL:
        return "EC_KERNEL";
    case EC_PART_ID_OVERFLOW:
        return "EC_PART_ID_OVERFLOW";
    case EC_ASYNC_TERMINATED:
        return "EC_ASYNC_TERMINATED";
    case EC_PART_CANCEL:
        return "EC_PART_CANCEL";        
    case EC_UNKNOWN:
        return "EC_UNKNOWN";
    default:
        return "EC_INVALID";
    }
}

const char *CommonUtil::getPortQueueType(PortQueueType type) {
    switch (type) {
    case PQT_LOCAL_REMOTE_SERVER_1:
        return "PQT_LOCAL_REMOTE_SERVER_1";
    case PQT_LOCAL_REMOTE_CLIENT_2:
        return "PQT_LOCAL_REMOTE_CLIENT_2";
    case PQT_LOCAL_LOCAL_3:
        return "PQT_LOCAL_LOCAL_3";
    case PQT_REMOTE_LOCAL_SERVER_4:
        return "PQT_REMOTE_LOCAL_SERVER_4";
    case PQT_REMOTE_LOCAL_CLIENT_5:
        return "PQT_REMOTE_LOCAL_CLIENT_5";
    case PQT_REMOTE_REMOTE_CLIENT_6:
        return "PQT_REMOTE_REMOTE_CLIENT_6";
    case PQT_UNKNOWN:
        return "PQT_UNKNOWN";
    default:
        assert(false);
        return "PQT_UNKNOWN_ERROR";
    }
}

const char *CommonUtil::getStoreType(PortStoreType type) {
    switch (type) {
    case PST_DATA:
        return "PST_DATA";
    case PST_SERIALIZE_DATA:
        return "PST_SERIALIZE_DATA";
    case PST_UNKNOWN:
        return "PST_UNKNOWN";
    default:
        assert(false);
        return "PST_UNKNOWN_ERROR";
    }
}

const char *CommonUtil::getIoType(IoType type) {
    switch (type) {
    case IOT_INPUT:
        return "input";
    case IOT_OUTPUT:
        return "output";
    default:
        assert(false);
        return "unknown";
    }
}

IoType CommonUtil::getReverseIoType(IoType type) {
    switch (type) {
    case IOT_INPUT:
        return IOT_OUTPUT;
    case IOT_OUTPUT:
        return IOT_INPUT;
    default:
        assert(false);
        return IOT_INPUT;
    }
}

uint64_t CommonUtil::random64() {
    multi_call::RandomGenerator generator(0);
    generator.get();
    return generator.get();
}

std::string CommonUtil::formatInstanceId(InstanceId instance) {
    constexpr uint32_t maxLength = 40;
    char buffer[maxLength];
    snprintf(buffer, maxLength, "%x", (uint32_t)instance);
    return std::string(buffer);
}

std::string CommonUtil::formatSessionId(SessionId id, InstanceId thisInstance) {
    constexpr uint32_t maxLength = 40;
    char buffer[maxLength];
    if (id.instance == thisInstance) {
        snprintf(buffer, maxLength, "%x-%d", (uint32_t)id.instance,
                 id.queryId);
    } else {
        snprintf(buffer, maxLength, "%x-%d-%x", (uint32_t)id.instance,
                 id.queryId, thisInstance);
    }
    return std::string(buffer);
}

std::string CommonUtil::getConfigAbsPath(const std::string &path,
                                         const std::string &file)
{
    if ('/' == file[0] || path.empty()) {
        return file;
    }
    if (path[path.length() - 1] == '/') {
        return path + file;
    } else {
        return path + '/' + file;
    }
}

std::string CommonUtil::getMirrorBizName(GraphId graphId) {
    return NAVI_SERVER_MIRROR_BIZ_PREFIX + autil::StringUtil::toString(graphId);
}

}
