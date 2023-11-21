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
#include <unordered_set>

#include "indexlib/common_define.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/doc_reader_base.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_doc_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(index, KVMergeWriter);
DECLARE_REFERENCE_CLASS(util, ProgressMetrics);
DECLARE_REFERENCE_CLASS(index, KVTTLDecider);
DECLARE_REFERENCE_STRUCT(index_base, SegmentTopologyInfo);

namespace indexlib { namespace index {

class KVMerger
{
public:
    KVMerger();
    ~KVMerger();

public:
    void Init(const index_base::PartitionDataPtr& partData, const config::IndexPartitionSchemaPtr& schema,
              int64_t currentTs);
    void Merge(const file_system::DirectoryPtr& segmentDir, const file_system::DirectoryPtr& indexDir,
               const index_base::SegmentDataVector& segmentDataVec,
               const index_base::SegmentTopologyInfo& targetTopoInfo);
    int64_t EstimateMemoryUse(const index_base::SegmentDataVector& segmentDataVec,
                              const index_base::SegmentTopologyInfo& targetTopoInfo) const;

    void SetMergeItemMetrics(const util::ProgressMetricsPtr& itemMetrics) { mMergeItemMetrics = itemMetrics; }

private:
    void GenerateSegmentIds();
    bool GetSegmentDocs();
    void MergeOneSegment(const index_base::SegmentData& segmentData, const KVMergeWriterPtr& writer,
                         bool isBottomLevel);
    void Dump(const KVMergeWriterPtr& writer, const file_system::DirectoryPtr& indexDir, const std::string& groupName);
    bool NeedCompactBucket(const config::KVIndexConfigPtr& kvConfig, const framework::SegmentMetricsVec& segmentMetrics,
                           const index_base::SegmentTopologyInfo& targetTopoInfo);

    static void LoadSegmentMetrics(const index_base::SegmentDataVector& segmentDataVec,
                                   framework::SegmentMetricsVec& segmentMetricsVec);

    static size_t CalculateTotalProgress(const std::string& groupName,
                                         const framework::SegmentMetricsVec& segmentMetricsVec);

private:
    std::unordered_set<keytype_t> mRemovedKeySet;
    config::KVIndexConfigPtr mKVIndexConfig;
    HashFunctionType mKeyHasherType;
    config::AttributeConfigPtr mPkeyAttrConfig;

    config::IndexPartitionSchemaPtr mSchema;
    KVTTLDeciderPtr mTTLDecider;
    int64_t mCurrentTsInSecond;
    util::ProgressMetricsPtr mMergeItemMetrics;

    size_t mTotalProgress;
    size_t mCurrentProgress;

    index_base::PartitionDataPtr mPartData;
    bool mNeedMerge = true;
    bool mNeedStorePKeyValue = false;

    // for pkey attribute
    std::vector<segmentid_t> mSegmentIds;
    std::vector<size_t> mSegmentDocCountVec;
    std::vector<index_base::SegmentData> mSegmentDataVec;
    size_t mCurrentSegmentIdx = 0;
    docid_t mCurrentDocId = 0;
    KVDocReader mDocReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVMerger);
}} // namespace indexlib::index
