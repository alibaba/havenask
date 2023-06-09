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
#ifndef __INDEXLIB_VAR_LEN_DATA_MERGER_H
#define __INDEXLIB_VAR_LEN_DATA_MERGER_H

#include <memory>
#include <unordered_set>

#include "indexlib/common_define.h"
#include "indexlib/index/data_structure/var_len_data_param.h"
#include "indexlib/index/data_structure/var_len_data_reader.h"
#include "indexlib/index/data_structure/var_len_data_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info_heap.h"
#include "indexlib/index/util/merger_resource.h"
#include "indexlib/index/util/segment_output_mapper.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class VarLenDataMerger
{
public:
    struct OutputData {
        VarLenDataWriterPtr dataWriter;
        segmentindex_t targetSegmentIndex = 0;
    };

    struct InputData {
        VarLenDataReaderPtr dataReader;
        AttributePatchReaderPtr segPatchReader;
        index_base::SegmentMergeInfo segMergeInfo;
    };

    struct OffsetPair {
        OffsetPair(uint64_t _oldOffset, uint64_t _newOffset, docid_t _oldDocId, uint16_t _targetSegIdx)
            : oldOffset(_oldOffset)
            , newOffset(_newOffset)
            , oldDocId(_oldDocId)
            , targetSegmentIndex(_targetSegIdx)
        {
        }

        ~OffsetPair() {}
        bool operator<(const OffsetPair& other) const
        {
            if (oldOffset == other.oldOffset) {
                return targetSegmentIndex < other.targetSegmentIndex;
            }
            return oldOffset < other.oldOffset;
        }
        bool operator==(const OffsetPair& other) const
        {
            return oldOffset == other.oldOffset && targetSegmentIndex == other.targetSegmentIndex;
        }
        bool operator!=(const OffsetPair& other) const { return !(*this == other); }
        uint64_t oldOffset;
        uint64_t newOffset;
        docid_t oldDocId;
        uint16_t targetSegmentIndex;
    };
    typedef std::vector<OffsetPair> SegmentOffsetMap;   // ordered vector
    typedef std::vector<SegmentOffsetMap> OffsetMapVec; // each segment
    typedef std::unordered_set<docid_t> DocIdSet;

public:
    VarLenDataMerger(const VarLenDataParam& param);
    ~VarLenDataMerger();

public:
    void Init(const MergerResource& resource, const std::vector<InputData>& inputReaders,
              const std::vector<OutputData>& outputWriters);

    size_t EstimateMemoryUse(uint32_t docCount);
    void Merge();
    const std::vector<OutputData>& GetOutputDatas() const { return mOutputDatas; }

private:
    void NormalMerge();
    void UniqMerge();
    void CloseOutputDatas();

    void ConstructSegmentOffsetMap(const index_base::SegmentMergeInfo& segMergeInfo, const ReclaimMapPtr& reclaimMap,
                                   const VarLenDataReaderPtr& segReader, SegmentOffsetMap& segmentOffsetMap);

    void MergeOneSegmentData(const VarLenDataReaderPtr& reader, SegmentOffsetMap& segmentOffsetMap,
                             const ReclaimMapPtr& reclaimMap, const index_base::SegmentMergeInfo& mergeSegInfo);

    void MergeDocOffsets(const ReclaimMapPtr& reclaimMap, const OffsetMapVec& offsetMapVec);

    uint64_t FindOrGenerateNewOffset(const autil::StringView& value, OutputData& output);

    void ReserveMergedOffsets();
    void ResetPoolBuffer();

private:
    VarLenDataParam mParam;
    DocumentMergeInfoHeap mHeap;
    SegmentOutputMapper<OutputData> mSegOutputMapper;
    std::vector<InputData> mInputDatas;
    std::vector<OutputData> mOutputDatas;
    DocIdSet mPatchDocIdSet;
    autil::mem_pool::Pool mPool;
    static constexpr size_t DEFAULT_POOL_BUFFER_SIZE = 2 * 1024 * 1024;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarLenDataMerger);
}} // namespace indexlib::index

#endif //__INDEXLIB_VAR_LEN_DATA_MERGER_H
