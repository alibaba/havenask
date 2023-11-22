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
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_redo_hint.h"
#include "indexlib/partition/operation_queue/operation_redo_strategy.h"
#include "indexlib/partition/operation_queue/remove_operation_creator.h"
#include "indexlib/util/Bitmap.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
namespace indexlib { namespace partition {

class ReopenOperationRedoStrategy : public OperationRedoStrategy
{
public:
    ReopenOperationRedoStrategy();
    ~ReopenOperationRedoStrategy();

public:
    void Init(const index_base::PartitionDataPtr& newPartitionData, const index_base::Version& lastVersion,
              const config::OnlineConfig& onlineConfig, const config::IndexPartitionSchemaPtr& schema);

    bool NeedRedo(segmentid_t operationSegment, OperationBase* operation, OperationRedoHint& redoHint);
    const OperationRedoCounter& GetCounter() const { return mRedoCounter; }
    const std::set<segmentid_t>& GetSkipDeleteSegments() const;

private:
    static int64_t GetMaxTimestamp(const index_base::PartitionDataPtr& partitionData);
    void InitSegmentMap(const index_base::Version& newVersion, const index_base::Version& lastVersion);
    index::PrimaryKeyIndexReaderPtr LoadPrimaryKeyIndexReader(const index_base::PartitionDataPtr& partitionData,
                                                              const config::IndexPartitionSchemaPtr& schema);

private:
    bool mIsIncConsistentWithRt;
    int64_t mMaxTsInNewVersion;
    util::Bitmap mBitmap;
    std::set<segmentid_t> mSkipDelOpSegments;
    bool mHasSubSchema;
    index::PrimaryKeyIndexReaderPtr mDiffPkReader;
    index::PartitionInfoPtr mDiffPartitionInfo;
    RemoveOperationCreatorPtr mRmOpCreator;
    InvertedIndexType mPkIndexType;
    OperationRedoCounter mRedoCounter;
    bool mEnableRedoSpeedup;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReopenOperationRedoStrategy);
}} // namespace indexlib::partition
