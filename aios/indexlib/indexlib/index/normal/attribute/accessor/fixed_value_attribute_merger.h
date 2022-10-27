#ifndef __INDEXLIB_FIXED_VALUE_ATTRIBUTE_MERGER_H
#define __INDEXLIB_FIXED_VALUE_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/segment_output_mapper.h"
#include "indexlib/util/profiling.h"
#include <fslib/fslib.h>

DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);

IE_NAMESPACE_BEGIN(index);

class FixedValueAttributeMerger : public AttributeMerger
{
protected:
    typedef std::vector<std::pair<std::string, segmentid_t> > PatchFileList;

    struct OutputData
    {
        size_t outputIdx = 0;
        size_t targetSegmentIndex = 0;
        uint32_t bufferSize = 0;
        uint32_t bufferCursor = 0;
        uint8_t* dataBuffer = nullptr;
        file_system::FileWriterPtr dataFileWriter;
        OutputData() = default;

        bool operator==(const OutputData& other) const {
            return targetSegmentIndex == other.targetSegmentIndex
                && bufferSize == other.bufferSize
                && bufferCursor == other.bufferCursor
                && dataBuffer == other.dataBuffer
                && dataFileWriter == other.dataFileWriter;
        }
    };

    public : FixedValueAttributeMerger(bool needMergePatch);
    ~FixedValueAttributeMerger();
    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(FixedValue);
public:
    void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;


protected:
    virtual void MergeSegment(
        const MergerResource& resource,
        const index_base::SegmentMergeInfo& segMergeInfo,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        const config::AttributeConfigPtr& attrConfig) = 0;

    virtual file_system::FileWriterPtr CreateDataFileWriter(
        const file_system::DirectoryPtr& attributeDir);

    virtual void CreateSegmentReaders(const index_base::SegmentMergeInfos& segMergeInfos,
            std::vector<AttributeSegmentReaderPtr>& segReaders) = 0;

    virtual void PrepareCompressOutputData(
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
    {
        assert(false);
    }
    virtual void FlushCompressDataBuffer(OutputData& outputData) { assert(false); }
    virtual void DumpCompressDataBuffer()
    { assert(false); }

    void FlushDataBuffer(OutputData& outputData);

    void PrepareOutputDatas(const ReclaimMapPtr& reclaimMap,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);
    void DestroyBuffers();
    void CloseFiles();

protected:
    void MergeData(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);


    void MergePatches(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

protected:
    static const uint32_t DEFAULT_RECORD_COUNT = 1024 * 1024;
    uint32_t mRecordSize;
    SegmentOutputMapper<OutputData> mSegOutputMapper;

private:
    friend class FixedValueAttributeMergerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FixedValueAttributeMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FIXED_VALUE_ATTRIBUTE_MERGER_H
