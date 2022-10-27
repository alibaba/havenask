#ifndef __INDEXLIB_DOCID_JOIN_VALUE_ATTRIBUTE_MERGER_H
#define __INDEXLIB_DOCID_JOIN_VALUE_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/file_system/file_writer.h"

IE_NAMESPACE_BEGIN(index);

class DocidJoinValueAttributeMerger : public AttributeMerger
{
public:
    DocidJoinValueAttributeMerger();
    ~DocidJoinValueAttributeMerger();
    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(DocidJoinValue);
public:
    void Init(const config::AttributeConfigPtr& attrConfig) override;

    void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void SortByWeightMerge(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge) const override;

    virtual bool EnableMultiOutputSegmentParallel() const { return false; }

private:
    file_system::FileWriterPtr CreateDataFileWriter(
        const file_system::DirectoryPtr& attributeDir);
    void MainToSubMerge(
        const MergerResource& resource, const std::vector<file_system::FileWriterPtr>& dataFiles);
    void SubToMainMerge(
        const MergerResource& resource, const std::vector<file_system::FileWriterPtr>& dataFiles);

private:
    static docid_t GetBaseDocId(const std::vector<docid_t>& docids, size_t segIdx)
    {
        if (docids.empty())
        {
            return 0;
        }
        return docids[segIdx];
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocidJoinValueAttributeMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOCID_JOIN_VALUE_ATTRIBUTE_MERGER_H
