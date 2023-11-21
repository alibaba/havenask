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

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info_heap.h"
#include "indexlib/index/normal/attribute/accessor/uniq_encoded_var_num_attribute_merger.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class UniqEncodedPackAttributeMerger : public UniqEncodedVarNumAttributeMerger<char>
{
private:
    using UniqEncodedVarNumAttributeMerger<char>::Init;
    using UniqEncodedVarNumAttributeMerger<char>::mSegOutputMapper;
    using UniqEncodedVarNumAttributeMerger<char>::mDataBuf;
    using VarNumAttributeMerger<char>::SegmentReaderWithCtx;

public:
#pragma pack(push, 1)
    struct PatchedDocInfo {
        docid_t newDocId; // docId in  current merged segment
        docid_t oldDocId; // docId in src segment
        int32_t segIdx;
        size_t offset;
        size_t dataLen;
        uint8_t data[0];
    };
#pragma pack(pop)

    typedef std::vector<PatchedDocInfo*> PatchedDocInfoVec;
    struct PatchDocInfoComparator {
        bool operator()(PatchedDocInfo* lhs, PatchedDocInfo* rhs)
        {
            if (lhs->segIdx != rhs->segIdx) {
                return lhs->segIdx < rhs->segIdx;
            }
            if (lhs->offset != rhs->offset) {
                return lhs->offset < rhs->offset;
            }
            return lhs->newDocId < rhs->newDocId;
        }
    };

public:
    UniqEncodedPackAttributeMerger(bool needMergePatch, size_t patchBufferSize);
    ~UniqEncodedPackAttributeMerger();

public:
    void Init(const config::PackAttributeConfigPtr& packAttrConf, const index_base::MergeItemHint& hint,
              const index_base::MergeTaskResourceVector& taskResources);

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

private:
    void MergePatchesInMergingSegments(const index_base::SegmentMergeInfos& segMergeInfos,
                                       const ReclaimMapPtr& reclaimMap, DocIdSet& patchedGlobalDocIdSet) override;

    void MergePatches(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                      const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    AttributePatchReaderPtr CreatePatchReaderForSegment(segmentid_t segmentId) override;

    void ReserveMemBuffers(const std::vector<AttributePatchReaderPtr>& patchReaders,
                           const std::vector<SegmentReaderWithCtx>& segReaders) override;

    void ReleaseMemBuffers() override;

    void GeneratePatchedDocInfos(const index_base::SegmentMergeInfos& segMergeInfos, DocumentMergeInfoHeap& heap,
                                 const std::vector<AttributePatchReaderPtr>& patchReaders,
                                 DocIdSet& patchedGlobalDocIdSet, PatchedDocInfoVec& docInfoVec,
                                 const ReclaimMapPtr& reclaimMap);

    void MergePatchedDocInfoAndDump(const index_base::SegmentMergeInfos& segMergeInfos,
                                    const PatchedDocInfoVec& docInfoVec, const ReclaimMapPtr& reclaimMap);

    void MergePatchAndDumpData(const common::PackAttributeFormatter::PackAttributeFields& patchFields, uint32_t dataLen,
                               docid_t docIdInNewSegment, OutputData& output);

    PatchedDocInfo* AllocatePatchDocInfo(size_t cur);

    bool FillPatchDocInfo(docid_t docIdInNewSegment, const DocumentMergeInfo& docMergeInfo,
                          const std::vector<AttributePatchReaderPtr>& patchReaders, PatchedDocInfo* patchDocInfo);

private:
    config::PackAttributeConfigPtr mPackAttrConfig;
    common::PackAttributeFormatterPtr mPackAttrFormatter;
    common::AttributeConvertorPtr mDataConvertor;
    util::MemBuffer mPatchBuffer;
    util::MemBuffer mMergeBuffer;
    size_t mBufferThreshold;
    size_t mPatchBufferSize;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UniqEncodedPackAttributeMerger);
}} // namespace indexlib::index
