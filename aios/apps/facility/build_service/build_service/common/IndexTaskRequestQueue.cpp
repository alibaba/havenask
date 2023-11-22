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
#include "build_service/common/IndexTaskRequestQueue.h"

#include <algorithm>

namespace build_service { namespace common {

std::vector<indexlibv2::framework::IndexTaskMeta>
IndexTaskRequestQueue::operator[](const std::string& partitionLevelKey) const
{
    auto iter = _partitionLevelRequestQueues.find(partitionLevelKey);
    if (iter != _partitionLevelRequestQueues.end()) {
        return iter->second;
    }
    return {};
}

indexlib::Status IndexTaskRequestQueue::add(const std::string& clusterName, const proto::Range& range,
                                            const indexlibv2::framework::IndexTaskMeta& newIndexTaskMeta)
{
    std::string key = genPartitionLevelKey(clusterName, range);
    auto& requestQueue = _partitionLevelRequestQueues[key];
    for (const auto& request : requestQueue) {
        if (request.GetTaskType() != newIndexTaskMeta.GetTaskType() ||
            request.GetTaskTraceId() != newIndexTaskMeta.GetTaskTraceId()) {
            continue;
        }
        if (request != newIndexTaskMeta) {
            return indexlib::Status::InvalidArgs(
                "cluster[%s], range[%u_%u], task type[%s], task trace id[%s], request already exists, %s",
                clusterName.c_str(), range.from(), range.to(), newIndexTaskMeta.GetTaskType().c_str(),
                newIndexTaskMeta.GetTaskTraceId().c_str(),
                autil::legacy::ToJsonString(newIndexTaskMeta, /*isCompact=*/true).c_str());
        }
        // ignore duplicate command
        return indexlib::Status::OK();
    }
    requestQueue.push_back(newIndexTaskMeta);
    auto cmp = [](const indexlibv2::framework::IndexTaskMeta& lhs,
                  const indexlibv2::framework::IndexTaskMeta& rhs) -> bool {
        return lhs.GetEventTimeInSecs() < rhs.GetEventTimeInSecs();
    };
    std::sort(requestQueue.begin(), requestQueue.end(), cmp);
    return indexlib::Status::OK();
}

void IndexTaskRequestQueue::remove(const std::string& clusterName, const proto::Range& range,
                                   const std::string& taskType, const std::string& taskTraceId)
{
    std::string key = genPartitionLevelKey(clusterName, range);
    auto& requestQueue = _partitionLevelRequestQueues[key];
    for (auto iter = requestQueue.begin(); iter != requestQueue.end(); ++iter) {
        if (iter->GetTaskType() == taskType && iter->GetTaskTraceId() == taskTraceId) {
            requestQueue.erase(iter);
            return;
        }
    }
}

std::string IndexTaskRequestQueue::genPartitionLevelKey(const std::string& clusterName, const proto::Range& range)
{
    return clusterName + "_" + autil::StringUtil::toString(range.from()) + "_" +
           autil::StringUtil::toString(range.to());
}

}} // namespace build_service::common
