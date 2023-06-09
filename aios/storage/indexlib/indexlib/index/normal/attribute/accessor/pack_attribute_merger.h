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
#ifndef __INDEXLIB_PACK_ATTRIBUTE_MERGER_H
#define __INDEXLIB_PACK_ATTRIBUTE_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/MemBuffer.h"

DECLARE_REFERENCE_CLASS(common, PackAttributeFormatter);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
namespace indexlib { namespace index {

class PackAttributeMerger : public VarNumAttributeMerger<char>
{
private:
    using VarNumAttributeMerger<char>::Init;
    using VarNumAttributeMerger<char>::SegmentReaderWithCtx;

public:
    PackAttributeMerger(bool needMergePatch = false);
    ~PackAttributeMerger();

public:
    void Init(const config::PackAttributeConfigPtr& packAttrConf, const index_base::MergeItemHint& hint,
              const index_base::MergeTaskResourceVector& taskResources);

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

private:
    AttributePatchReaderPtr CreatePatchReaderForSegment(segmentid_t segmentId) override;

    uint32_t ReadData(docid_t docId, const SegmentReaderWithCtx& segReaders, const AttributePatchReaderPtr& patchReader,
                      uint8_t* dataBuf, uint32_t dataBufLen) override;

    void MergePatches(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                      const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void ReserveMemBuffers(const std::vector<AttributePatchReaderPtr>& patchReaders,
                           const std::vector<SegmentReaderWithCtx>& segReaders) override;

    void ReleaseMemBuffers() override;

private:
    config::PackAttributeConfigPtr mPackAttrConfig;
    common::PackAttributeFormatterPtr mPackAttrFormatter;
    common::AttributeConvertorPtr mDataConvertor;
    mutable util::MemBuffer mMergeBuffer;
    mutable util::MemBuffer mPatchBuffer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeMerger);
}} // namespace indexlib::index

#endif //__INDEXLIB_PACK_ATTRIBUTE_MERGER_H
