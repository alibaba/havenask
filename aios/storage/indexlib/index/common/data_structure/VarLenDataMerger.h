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

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/attribute/merger/DocumentMergeInfoHeap.h"
#include "indexlib/index/attribute/merger/SegmentOutputMapper.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"

namespace indexlibv2::index {
class VarLenDataWriter;
class VarLenDataReader;
class AttributePatchReader;
class DocMapper;

class VarLenDataMerger
{
public:
    struct OutputData {
        std::shared_ptr<VarLenDataWriter> dataWriter;
        segmentid_t targetSegmentId = 0;
    };

    struct InputData {
        std::shared_ptr<VarLenDataReader> dataReader;
        std::shared_ptr<AttributePatchReader> segPatchReader;
    };

    struct OffsetPair {
        OffsetPair(uint64_t _oldOffset, uint64_t _newOffset, docid_t _oldDocId, uint16_t _targetSegId)
            : oldOffset(_oldOffset)
            , newOffset(_newOffset)
            , oldDocId(_oldDocId)
            , targetSegmentId(_targetSegId)
        {
        }

        ~OffsetPair() {}
        bool operator<(const OffsetPair& other) const
        {
            if (oldOffset == other.oldOffset) {
                return targetSegmentId < other.targetSegmentId;
            }
            return oldOffset < other.oldOffset;
        }
        bool operator==(const OffsetPair& other) const
        {
            return oldOffset == other.oldOffset && targetSegmentId == other.targetSegmentId;
        }
        bool operator!=(const OffsetPair& other) const { return !(*this == other); }
        uint64_t oldOffset;
        uint64_t newOffset;
        docid_t oldDocId;
        uint16_t targetSegmentId;
    };
    using SegmentOffsetMap = std::vector<OffsetPair>;   // ordered vector
    using OffsetMapVec = std::vector<SegmentOffsetMap>; // each segment
    using DocIdSet = std::unordered_set<docid_t>;

public:
    VarLenDataMerger(const VarLenDataParam& param);
    ~VarLenDataMerger();

public:
    void Init(const IIndexMerger::SegmentMergeInfos& segMergeInfos, const std::shared_ptr<DocMapper>& docMapper,
              const std::vector<InputData>& inputReaders, const std::vector<OutputData>& outputWriters);

    size_t EstimateMemoryUse(uint32_t docCount);
    Status Merge();
    const std::vector<OutputData>& GetOutputDatas() const { return _outputDatas; }

private:
    Status NormalMerge();
    Status UniqMerge();
    Status CloseOutputDatas();

    Status ConstructSegmentOffsetMap(const IIndexMerger::SourceSegment& sourceSegmentInfo,
                                     const std::shared_ptr<DocMapper>& docMapper,
                                     const std::shared_ptr<VarLenDataReader>& segReader,
                                     SegmentOffsetMap& segmentOffsetMap);

    Status MergeOneSegmentData(const std::shared_ptr<VarLenDataReader>& reader, SegmentOffsetMap& segmentOffsetMap,
                               const std::shared_ptr<DocMapper>& docMapper,
                               const IIndexMerger::SourceSegment& sourceSegmentInfo);

    Status MergeDocOffsets(const std::shared_ptr<DocMapper>& docMapper, const OffsetMapVec& offsetMapVec);

    std::pair<Status, uint64_t> FindOrGenerateNewOffset(const autil::StringView& value, OutputData& output);

    Status ReserveMergedOffsets();
    void ResetPoolBuffer();

private:
    IIndexMerger::SegmentMergeInfos _segmentMergeInfos;
    VarLenDataParam _param;
    DocumentMergeInfoHeap _heap;
    SegmentOutputMapper<OutputData> _segOutputMapper;
    std::vector<InputData> _inputDatas;
    std::vector<OutputData> _outputDatas;
    DocIdSet _patchDocIdSet;
    autil::mem_pool::Pool _pool;
    static constexpr size_t DEFAULT_POOL_BUFFER_SIZE = 2 * 1024 * 1024;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
