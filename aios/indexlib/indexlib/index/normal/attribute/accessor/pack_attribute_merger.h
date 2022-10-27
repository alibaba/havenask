#ifndef __INDEXLIB_PACK_ATTRIBUTE_MERGER_H
#define __INDEXLIB_PACK_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger.h"
#include "indexlib/util/mem_buffer.h"

DECLARE_REFERENCE_CLASS(common, PackAttributeFormatter);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
IE_NAMESPACE_BEGIN(index);

class PackAttributeMerger : public VarNumAttributeMerger<char>
{
private:
    using VarNumAttributeMerger<char>::Init;

public:
    PackAttributeMerger(bool needMergePatch = false);
    ~PackAttributeMerger();

public:
    void Init(const config::PackAttributeConfigPtr& packAttrConf,
              const index_base::MergeItemHint& hint,
              const index_base::MergeTaskResourceVector& taskResources);

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge) const override;

private:
    AttributePatchReaderPtr CreatePatchReaderForSegment(segmentid_t segmentId) override;

    uint32_t ReadData(docid_t docId,
                      const AttributeSegmentReaderPtr &attributeSegmentReader,
                      const AttributePatchReaderPtr &patchReader, 
                      uint8_t* dataBuf, uint32_t dataBufLen) override;

    void MergePatches(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void ReserveMemBuffers(
            const std::vector<AttributePatchReaderPtr>& patchReaders,
            const std::vector<AttributeSegmentReaderPtr>& segReaders) override;

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

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_ATTRIBUTE_MERGER_H
