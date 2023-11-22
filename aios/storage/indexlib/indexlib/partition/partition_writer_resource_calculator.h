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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/operation_queue/operation_writer.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);

namespace indexlib { namespace partition {

class PartitionWriterResourceCalculator
{
public:
    PartitionWriterResourceCalculator(const config::IndexPartitionOptions& options);
    PartitionWriterResourceCalculator(bool isOnline, int64_t dumpThreadCount);

    ~PartitionWriterResourceCalculator() {}

public:
    void Init(const index_base::SegmentWriterPtr& segWriter, const util::BuildResourceMetricsPtr& modifierMetrics,
              const OperationWriterPtr& operationWriter);
    void Init(const util::BuildResourceMetricsPtr& segMetrics, const util::BuildResourceMetricsPtr& modifierMetrics,
              const util::BuildResourceMetricsPtr& operationMetrics);

    int64_t GetCurrentMemoryUse() const;
    int64_t EstimateMaxMemoryUseOfCurrentSegment() const;
    int64_t EstimateDumpMemoryUse() const;
    int64_t EstimateDumpFileSize() const;

private:
    bool mIsOnline = true;
    int64_t mDumpThreadCount = 0;
    util::BuildResourceMetricsPtr mSegWriterMetrics;
    util::BuildResourceMetricsPtr mModifierMetrics;
    util::BuildResourceMetricsPtr mOperationWriterMetrics;

private:
    friend class PartitionWriterResourceCalculatorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionWriterResourceCalculator);
}} // namespace indexlib::partition
