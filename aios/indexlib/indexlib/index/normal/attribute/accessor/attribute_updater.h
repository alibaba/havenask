#ifndef __INDEXLIB_ATTRIBUTE_UPDATER_H
#define __INDEXLIB_ATTRIBUTE_UPDATER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/index_define.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/util/buffer_compressor/buffer_compressor_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/snappy_compress_file_writer.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/util/simple_pool.h"

IE_NAMESPACE_BEGIN(index);

class AttributeUpdater
{
public:
    AttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics,
                     segmentid_t segId, 
                     const config::AttributeConfigPtr& attrConfig) 
        : mBuildResourceMetrics(buildResourceMetrics)
        , mBuildResourceMetricsNode(NULL)
        , mSegmentId(segId)
        , mAttrConfig(attrConfig)
    {
        if (mBuildResourceMetrics)
        {
            mBuildResourceMetricsNode = mBuildResourceMetrics->AllocateNode();
            IE_LOG(INFO, "allocate build resource node [id:%d] for AttributeUpdater[%s] in segment[%d]",
                   mBuildResourceMetricsNode->GetNodeId(),
                   mAttrConfig->GetAttrName().c_str(),
                   segId);
        }
    }
    virtual ~AttributeUpdater() {}

public:
    static const size_t DEFAULT_COMPRESS_BUFF_SIZE = 10 * 1024 * 1024; // 10MB
    
public:
    virtual void Update(docid_t docId, const autil::ConstString& attributeValue) = 0;
    virtual void Dump(
            const file_system::DirectoryPtr& attributeDir, segmentid_t srcSegment) = 0;
    virtual file_system::FileWriterPtr CreatePatchFileWriter(
            const file_system::DirectoryPtr& directory,
            const std::string& fileName) 
    {        
        file_system::FileWriterPtr patchFileWriter = 
            directory->CreateFileWriter(fileName);
        if (!mAttrConfig->GetCompressType().HasPatchCompress())
        {
            return patchFileWriter;
        }
        file_system::SnappyCompressFileWriterPtr compressWriter(
                new file_system::SnappyCompressFileWriter);
        compressWriter->Init(patchFileWriter, DEFAULT_COMPRESS_BUFF_SIZE);
        return compressWriter;
    }

    int64_t EstimateDumpFileSize(int64_t valueSize)
    {
        if (!mAttrConfig->GetCompressType().HasPatchCompress())
        {
            return valueSize;
        }
        return valueSize * config::CompressTypeOption::PATCH_COMPRESS_RATIO;
    }

    int64_t GetPatchFileWriterBufferSize()
    {
        if (!mAttrConfig->GetCompressType().HasPatchCompress())
        {
            return 0;
        }
        // comperssor inbuffer + out buffer
        return DEFAULT_COMPRESS_BUFF_SIZE * 2;
    }

protected:
    std::string GetPatchFileName(segmentid_t srcSegment) const;

protected:
    util::BuildResourceMetrics* mBuildResourceMetrics;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    segmentid_t mSegmentId;
    config::AttributeConfigPtr mAttrConfig;
    util::SimplePool mSimplePool;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeUpdater);
//////////////////////////////////////////////////////////////////

inline std::string AttributeUpdater::GetPatchFileName(
        segmentid_t srcSegment) const
{
    std::stringstream ss;
    if (srcSegment == INVALID_SEGMENTID)
    {
        // TODO delete : support legacy patch name
        ss << mSegmentId << "." << ATTRIBUTE_PATCH_FILE_NAME;
    }
    else
    {
        ss << srcSegment << "_" << mSegmentId << "." << ATTRIBUTE_PATCH_FILE_NAME;
    }
    return ss.str();
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_UPDATER_H
