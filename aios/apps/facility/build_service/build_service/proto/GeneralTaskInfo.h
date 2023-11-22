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

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "indexlib/base/Constant.h"

namespace build_service::proto {

struct GeneralTaskInfo : public autil::legacy::Jsonizable {
    GeneralTaskInfo() = default;

    std::string taskType;
    std::string taskName;
    std::string taskTraceId;
    indexlibv2::versionid_t baseVersionId = indexlibv2::INVALID_VERSIONID;
    uint64_t branchId = 0;
    int64_t totalOpCount = -1;
    int64_t finishedOpCount = -1;
    std::string clusterName;
    std::string result;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("task_type", taskType, taskType);
        json.Jsonize("task_name", taskName, taskName);
        json.Jsonize("task_trace_id", taskTraceId, taskTraceId);
        json.Jsonize("base_version_id", baseVersionId, baseVersionId);
        json.Jsonize("branch_id", branchId, branchId);
        json.Jsonize("total_op_count", totalOpCount, totalOpCount);
        json.Jsonize("finished_op_count", finishedOpCount, finishedOpCount);
        json.Jsonize("cluster_name", clusterName, clusterName);
        json.Jsonize("result", result, result);
    }
};

} // namespace build_service::proto
