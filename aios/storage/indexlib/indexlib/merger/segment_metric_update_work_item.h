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

#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/index/segment_metrics_updater/multi_segment_metrics_updater.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/util/resource_control_work_item.h"

namespace indexlib { namespace merger {

class SegmentMetricUpdateWorkItem : public util::ResourceControlWorkItem
{
public:
    SegmentMetricUpdateWorkItem(index::MultiSegmentMetricsUpdaterPtr updater, const std::string& checkpointDir,
                                file_system::FenceContext* fenceContext)
        : mUpdater(updater)
        , mCheckpointDir(checkpointDir)
        , mFenceContext(fenceContext)
    {
    }

    ~SegmentMetricUpdateWorkItem();

public:
    void Process() override;
    void Destroy() override;
    int64_t GetRequiredResource() const override;

private:
    index::MultiSegmentMetricsUpdaterPtr mUpdater;
    std::string mCheckpointDir;
    file_system::FenceContext* mFenceContext;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentMetricUpdateWorkItem);
}} // namespace indexlib::merger
