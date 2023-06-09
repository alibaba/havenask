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
#ifndef ISEARCH_BS_BSMETRICTAGSHANDLER_H
#define ISEARCH_BS_BSMETRICTAGSHANDLER_H

#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"
#include "fslib/util/MetricTagsHandler.h"

namespace build_service { namespace util {

class BsMetricTagsHandler : public fslib::util::MetricTagsHandler
{
public:
    BsMetricTagsHandler(const proto::PartitionId& pid);
    ~BsMetricTagsHandler();

private:
    BsMetricTagsHandler(const BsMetricTagsHandler&);
    BsMetricTagsHandler& operator=(const BsMetricTagsHandler&);

public:
    void getTags(const std::string& filePath, kmonitor::MetricsTags& tags) const override;
    // begin, do, end
    void setMergePhase(proto::MergeStep phase);
    // full, inc, unknown
    void setBuildStep(proto::BuildStep buildStep);

private:
    proto::PartitionId _pid;
    std::string _partition;
    std::string _buildStep;
    std::string _clusterName;
    std::string _generationId;
    std::string _roleName;
    std::string _mergePhase;
    std::string _buildIdAppName;

private:
    static std::string SERVICE_NAME_PREFIX;

private:
    BS_LOG_DECLARE();
};

typedef std::shared_ptr<BsMetricTagsHandler> BsMetricTagsHandlerPtr;

}} // namespace build_service::util

#endif // ISEARCH_BS_BSMETRICTAGSHANDLER_H
