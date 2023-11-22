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

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/metrics/ProgressMetrics.h"
#include "indexlib/util/resource_control_work_item.h"
#include "kmonitor/client/core/MetricsTags.h"

namespace indexlib { namespace merger {

class MergeWorkItem : public util::ResourceControlWorkItem
{
public:
    MergeWorkItem(const MergeFileSystemPtr& mergeFileSystem)
        : mMergeFileSystem(mergeFileSystem)
        , mMetrics(NULL)
        , mCost(0.0)
    {
    }

    virtual ~MergeWorkItem() {}

public:
    virtual int64_t EstimateMemoryUse() const = 0;

    virtual void SetMergeItemMetrics(const util::ProgressMetricsPtr& itemMetrics) {}

    void Process() override;

    std::string GetIdentifier() const { return mIdentifier; }
    void SetIdentifier(const std::string& identifier) { mIdentifier = identifier; }

    const std::string& GetCheckPointFileName() const { return mCheckPointName; }
    void SetCheckPointFileName(const std::string& checkPointName) { mCheckPointName = checkPointName; }

    void SetMetrics(merger::IndexPartitionMergerMetrics* metrics, double cost, const kmonitor::MetricsTags& tags)
    {
        mMetrics = metrics;
        mCost = cost;
        if (mMetrics) {
            mItemMetrics = mMetrics->RegisterMergeItem(mCost, tags);
            SetMergeItemMetrics(mItemMetrics);
        }
    }

    void ReportMetrics()
    {
        if (mItemMetrics) {
            mItemMetrics->SetFinish();
        }
    }

    double GetCost() const { return mCost; }

    void SetCounter(const util::StateCounterPtr& counter) { mCounter = counter; }

protected:
    virtual void DoProcess() = 0;

protected:
    util::StateCounterPtr mCounter;
    MergeFileSystemPtr mMergeFileSystem;

private:
    std::string mIdentifier;
    std::string mCheckPointName;
    merger::IndexPartitionMergerMetrics* mMetrics;
    util::ProgressMetricsPtr mItemMetrics;
    double mCost;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(MergeWorkItem);
}} // namespace indexlib::merger
