#include <sstream>
#include <autil/ConstString.h>
#include "indexlib/index/normal/summary/local_disk_summary_writer.h"
#include "indexlib/util/profiling.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/util/mmap_allocator.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, LocalDiskSummaryWriter);

bool LocalDiskSummaryWriter::AddDocument(const ConstString& data)
{
    mAccessor->Append(data);
    UpdateBuildResourceMetrics();    
    return true;
}

void LocalDiskSummaryWriter::Dump(const file_system::DirectoryPtr& directory,
                                  autil::mem_pool::PoolBase* dumpPool)
{
    file_system::FileWriterPtr offsetWriter = directory->CreateFileWriter(
            SUMMARY_OFFSET_FILE_NAME);
    file_system::FileWriterPtr dataWriter = directory->CreateFileWriter(
            SUMMARY_DATA_FILE_NAME);

    offsetWriter->ReserveFileNode(GetNumDocuments() * sizeof(uint64_t));
    dataWriter->ReserveFileNode(mAccessor->GetAppendDataSize());

    VarNumAttributeDataIteratorPtr iter = 
        mAccessor->CreateVarNumAttributeDataIterator();
    uint64_t currentOffset = 0;
    while (iter->HasNext())
    {
        uint8_t* data = NULL;
        uint64_t dataLength = 0;

        iter->Next();
        iter->GetCurrentData(dataLength, data);
        dataWriter->Write(data, dataLength);
        offsetWriter->Write(&currentOffset, sizeof(uint64_t));
        currentOffset += dataLength;
    }

    assert(offsetWriter->GetLength() == GetNumDocuments() * sizeof(uint64_t));
    assert(dataWriter->GetLength() == mAccessor->GetAppendDataSize());
    offsetWriter->Close();
    dataWriter->Close();
}

void LocalDiskSummaryWriter::InitMemoryPool()
{
    mAllocator.reset(new util::MMapAllocator);        
    mPool.reset(new autil::mem_pool::Pool(mAllocator.get(),
                    DEFAULT_CHUNK_SIZE * 1024 * 1024));        
}

void LocalDiskSummaryWriter::Init(const SummaryGroupConfigPtr& summaryGroupConfig,
                                  BuildResourceMetrics* buildResourceMetrics)
{ 
    mSummaryGroupConfig = summaryGroupConfig;

    InitMemoryPool();
    assert(mPool);
    char* buffer = (char*)mPool->allocate(sizeof(SummaryDataAccessor));
    mAccessor = new (buffer) SummaryDataAccessor();
    mAccessor->Init(mPool.get());
    
    if (buildResourceMetrics)
    {
        mBuildResourceMetricsNode = buildResourceMetrics->AllocateNode();
        IE_LOG(INFO, "allocate build resource node [id:%d] for summary",
               mBuildResourceMetricsNode->GetNodeId());
        UpdateBuildResourceMetrics();
    }    
}

void LocalDiskSummaryWriter::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode)
    {
        return;
    }
    
    int64_t poolSize = mPool->getUsedBytes();
    int64_t dumpFileSize = poolSize;
    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, 0);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, 0);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

const SummarySegmentReaderPtr LocalDiskSummaryWriter::CreateInMemSegmentReader()
{
    return InMemSummarySegmentReaderPtr(
            new InMemSummarySegmentReader(mSummaryGroupConfig, mAccessor));
}

IE_NAMESPACE_END(index);
