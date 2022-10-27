#ifndef __INDEXLIB_LOCAL_DISK_SUMMARY_WRITER_H
#define __INDEXLIB_LOCAL_DISK_SUMMARY_WRITER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include <autil/ConstString.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/index/normal/summary/summary_writer.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"

DECLARE_REFERENCE_CLASS(util, MMapAllocator);
IE_NAMESPACE_BEGIN(index);

class LocalDiskSummaryWriter
{
public:
    LocalDiskSummaryWriter()
        : mAccessor(NULL)
        , mBuildResourceMetricsNode(NULL)
    {}
    
    ~LocalDiskSummaryWriter()
    {
        if (mAccessor)
        {
            mAccessor->~SummaryDataAccessor();
        }
        mPool.reset();
    }

public:
    void Init(const config::SummaryGroupConfigPtr& summaryGroupConfig,
              util::BuildResourceMetrics* buildResourceMetrics);
    bool AddDocument(const autil::ConstString& data);
    void Dump(const file_system::DirectoryPtr& directory,
              autil::mem_pool::PoolBase* dumpPool = NULL);
    const SummarySegmentReaderPtr CreateInMemSegmentReader();
    uint32_t GetNumDocuments() const { return mAccessor->GetDocCount(); }
    const std::string& GetGroupName() const
    { return mSummaryGroupConfig->GetGroupName(); }

private:
    void InitMemoryPool();
    void UpdateBuildResourceMetrics();

private:
    config::SummaryGroupConfigPtr mSummaryGroupConfig;
    SummaryDataAccessor* mAccessor;
    util::MMapAllocatorPtr mAllocator;    
    std::tr1::shared_ptr<autil::mem_pool::Pool> mPool;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode; 
    
private:
    friend class SummaryWriterTest;
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<LocalDiskSummaryWriter> LocalDiskSummaryWriterPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LOCAL_DISK_SUMMARY_WRITER_H
