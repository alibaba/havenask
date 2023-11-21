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
#include "build_service/proto/Admin.pb.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/IndexTaskQueue.h"

namespace build_service { namespace common {

class IndexTaskRequestQueue : public autil::legacy::Jsonizable
{
public:
    static std::string genPartitionLevelKey(const std::string& clusterName, const proto::Range& range);

    std::vector<indexlibv2::framework::IndexTaskMeta> operator[](const std::string& partitionLevelKey) const;
    bool operator==(const IndexTaskRequestQueue& other) const
    {
        return _partitionLevelRequestQueues == other._partitionLevelRequestQueues;
    }

    indexlib::Status add(const std::string& clusterName, const proto::Range& range,
                         const indexlibv2::framework::IndexTaskMeta& newIndexTaskMeta);

    void remove(const std::string& clusterName, const proto::Range& range, const std::string& taskType,
                const std::string& taskTraceId);
    size_t size() { return _partitionLevelRequestQueues.size(); }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("index_task_request_queue", _partitionLevelRequestQueues, _partitionLevelRequestQueues);
    }

private:
    // key : cluster_from_to
    std::map<std::string, std::vector<indexlibv2::framework::IndexTaskMeta>> _partitionLevelRequestQueues;
};

}} // namespace build_service::common
