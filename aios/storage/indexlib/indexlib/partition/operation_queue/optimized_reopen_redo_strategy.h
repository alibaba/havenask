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
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/operation_queue/operation_redo_strategy.h"

namespace indexlib { namespace partition {

class OptimizedReopenRedoStrategy : public OperationRedoStrategy
{
public:
    OptimizedReopenRedoStrategy();
    ~OptimizedReopenRedoStrategy();

public:
    void Init(const index_base::PartitionDataPtr& newPartitionData, index_base::Version& oldVersion,
              const config::IndexPartitionSchemaPtr& schema);
    bool NeedRedo(segmentid_t operationSegment, OperationBase* operation, OperationRedoHint& redoHint) override;
    const std::set<segmentid_t>& GetSkipDeleteSegments() const override
    {
        assert(false);
        // unexcepeted to be reached
        return mSkipDeleteSegments;
    }

private:
    void FillDocRange(index_base::Version::Iterator& iter,
                      const std::map<segmentid_t, std::pair<docid_t, docid_t>>& globalDocRange,
                      std::vector<std::pair<docid_t, docid_t>>& docRange);
    void CaculatorDocRange(const index_base::PartitionDataPtr& partitionData,
                           std::map<segmentid_t, std::pair<docid_t, docid_t>>& docRange);

private:
    std::vector<std::pair<docid_t, docid_t>> mDeleteDocRange;
    std::vector<std::pair<docid_t, docid_t>> mUpdateDocRange;
    std::vector<std::pair<docid_t, docid_t>> mSubDeleteDocRange;
    std::vector<std::pair<docid_t, docid_t>> mSubUpdateDocRange;
    std::set<segmentid_t> mSkipDeleteSegments;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OptimizedReopenRedoStrategy);
}} // namespace indexlib::partition
