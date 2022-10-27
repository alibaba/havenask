#ifndef __INDEXLIB_INDEX_OUTPUT_SEGMENT_RESOURCE_H
#define __INDEXLIB_INDEX_OUTPUT_SEGMENT_RESOURCE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/normal/inverted_index/accessor/index_data_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(util, SimplePool);

IE_NAMESPACE_BEGIN(index);

class IndexOutputSegmentResource
{
public:
    IndexOutputSegmentResource(size_t dictKeyCount = 0)
        : mDictKeyCount(dictKeyCount)
    {        
    }    
    ~IndexOutputSegmentResource();

public:
    void Init(const file_system::DirectoryPtr& mergeDir, const config::IndexConfigPtr& indexConfig,
              const config::MergeIOConfig& ioConfig, util::SimplePool* simplePool, 
              bool hasAdaptiveBitMap);
    void Reset();
    IndexDataWriterPtr& GetIndexDataWriter(SegmentTermInfo::TermIndexMode mode);

private:
    void CreateNormalIndexDataWriter(
            const config::IndexConfigPtr& indexConfig, 
            const config::MergeIOConfig& IOConfig,
            util::SimplePool* simplePool);
    void CreateBitmapIndexDataWriter(const config::IndexConfigPtr& indexConfig,
            const config::MergeIOConfig& IOConfig, util::SimplePool* simplePool,
            bool hasAdaptiveBitMap);
    void DestroyIndexDataWriters() 
    {
        FreeIndexDataWriter(mNormalIndexDataWriter);
        FreeIndexDataWriter(mBitmapIndexDataWriter);
    }
    
private:
    void FreeIndexDataWriter(IndexDataWriterPtr& writer)
    {
        if (writer)
        {
            if (writer->IsValid())
            {
                writer->dictWriter->Close();
                writer->postingWriter->Close();
            }
            writer->dictWriter.reset();
            writer->postingWriter.reset();
            writer.reset();
        }
    }


private:
    file_system::DirectoryPtr mMergeDir;
    IndexDataWriterPtr mNormalIndexDataWriter;
    IndexDataWriterPtr mBitmapIndexDataWriter;
    size_t mDictKeyCount;

private:
    friend class BitmapPostingMergerTest;
    friend class ExpackPostingMergerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexOutputSegmentResource);
typedef std::vector<IndexOutputSegmentResourcePtr> IndexOutputSegmentResources;
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_OUTPUT_SEGMENT_RESOURCE_H
