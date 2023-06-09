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

#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "autil/Log.h"
#include "suez/service/Service.pb.h"
namespace kmonitor {
class MetricsReporter;
}

namespace suez {

constexpr char kQps[] = "table_service.qps";
constexpr char kErrorQps[] = "table_service.errorQps";
constexpr char kLatency[] = "table_service.latency";
constexpr char kDocCount[] = "table_service.docCount";
constexpr char kQuerySchema[] = "querySchema";
constexpr char kQueryVersion[] = "queryVersion";
constexpr char kQueryTable[] = "queryTable"; // queryData is better.
constexpr char kWrite[] = "write";
constexpr char kUpdateSchema[] = "updateSchema";
constexpr char kGetSchemaVersion[] = "getSchemaVersion";
constexpr char kQueryType[] = "query_type";
constexpr char kTableName[] = "table_name";
constexpr char kKvBatchGet[] = "KvBatchGet";
constexpr char kUnknownTable[] = "unknown";

class TableServiceHelper {
public:
    static void trySetGigEc(google::protobuf::Closure *done, multi_call::MultiCallErrorCode gigEc);
    static void setErrorInfo(ErrorInfo *errInfo,
                             google::protobuf::Closure *done,
                             suez::ErrorCode errorCode,
                             std::string msg,
                             const char *queryType,
                             const std::string tableName,
                             kmonitor::MetricsReporter *metricsReporter);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace suez
