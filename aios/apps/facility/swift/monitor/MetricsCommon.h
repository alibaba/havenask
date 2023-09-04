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
#include <stdint.h>
#include <string>

namespace swift {
namespace monitor {

const static std::string READ_MSG_GROUP_METRIC = "partition.read.message.";
const static std::string READ_REQUEST_GROUP_METRIC = "partition.read.request.";
const static std::string WRITE_MSG_GROUP_METRIC = "partition.write.message.";
const static std::string WRITE_REQUEST_GROUP_METRIC = "partition.write.request.";
const static std::string WRITE_REQUEST_SECURITY_GROUP_METRIC = "partition.write.request.security.";
const static std::string PARTITION_GROUP_METRIC = "partition.other.";
const static std::string PARTITION_DFS_GROUP_METRIC = "partition.dfs.";
const static std::string PARTITION_MEMORY_GROUP_METRIC = "partition.memory.";
const static std::string PARTITION_ERROR_GROUP_METRIC = "partition.error.";
const static std::string BROKER_WORKER_GROUP_METRIC = "worker.broker.memory.";
const static std::string BROKER_WORKER_DFS_GROUP_METRIC = "worker.broker.dfs.";
const static std::string BROKER_WORKER_GROUP_STATUS_METRIC = "worker.broker.status.";
const static std::string WORKER_ERROR_GROUP_METRIC = "worker.error.";
const static std::string ADMIN_GROUP_METRIC = "worker.admin.";
const static std::string CLIENT_WRITER_METRIC = "swift_client.writer.";
const static std::string CLIENT_READER_METRIC = "swift_client.reader.";

#define SWIFT_REGISTER_QPS_METRIC(metricName, metricPrefix)                                                            \
    {                                                                                                                  \
        std::string name = metricPrefix;                                                                               \
        name += #metricName;                                                                                           \
        REGISTER_QPS_MUTABLE_METRIC(metricName, name);                                                                 \
    }

#define SWIFT_REGISTER_GAUGE_METRIC(metricName, metricPrefix)                                                          \
    {                                                                                                                  \
        std::string name = metricPrefix;                                                                               \
        name += #metricName;                                                                                           \
        REGISTER_GAUGE_MUTABLE_METRIC(metricName, name);                                                               \
    }

#define SWIFT_REGISTER_LATENCY_METRIC(metricName, metricPrefix)                                                        \
    {                                                                                                                  \
        std::string name = metricPrefix;                                                                               \
        name += #metricName;                                                                                           \
        REGISTER_LATENCY_MUTABLE_METRIC(metricName, name);                                                             \
    }

#define SWIFT_REGISTER_STATUS_METRIC(metricName, metricPrefix)                                                         \
    {                                                                                                                  \
        std::string name = metricPrefix;                                                                               \
        name += #metricName;                                                                                           \
        REGISTER_STATUS_MUTABLE_METRIC(metricName, name);                                                              \
    }

#define SWIFT_REPORT_MUTABLE_METRIC(metricName, value)                                                                 \
    {                                                                                                                  \
        if (value >= 0) {                                                                                              \
            REPORT_MUTABLE_METRIC(metricName, value);                                                                  \
        }                                                                                                              \
    }

#define SWIFT_REPORT_QPS_METRIC(metricName)                                                                            \
    { REPORT_MUTABLE_QPS(metricName); }

std::string intToString(uint32_t val);
} // namespace monitor
} // namespace swift
