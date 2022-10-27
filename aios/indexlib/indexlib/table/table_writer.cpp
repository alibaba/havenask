#include "indexlib/table/table_writer.h"
#include "indexlib/table/building_segment_reader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/index_base/segment/segment_data_base.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TableWriter);

TableWriter::TableWriter()
{
}

TableWriter::~TableWriter() 
{
}

bool TableWriter::Init(const TableWriterInitParamPtr& initParam)
{
    mRootDirectory = initParam->rootDirectory;
    mSegmentData = initParam->newSegmentData;
    mSegmentDataDirectory = initParam->segmentDataDirectory;
    mTableResource = initParam->tableResource;
    mSchema = initParam->schema;
    mOptions = initParam->options;
    mOnDiskVersion = initParam->onDiskVersion;
    mMemoryController = initParam->memoryController;
    mPartitionRange = initParam->partitionRange;
    bool initSuccess = DoInit();
    if (initSuccess)
    {
        mBuildingSegmentReader = CreateBuildingSegmentReader(mMemoryController);
        UpdateMemoryUse();
    }
    return initSuccess;
}

void TableWriter::UpdateMemoryUse()
{
    int64_t currentUse = GetCurrentMemoryUse();
    int64_t allocateMemUse = currentUse - mMemoryController->GetUsedQuota();
    if (allocateMemUse > 0)
    {
        mMemoryController->Allocate(allocateMemUse);
    }
}

bool TableWriter::IsFull() const
{
    return false;
}

segmentid_t TableWriter::GetBuildingSegmentId() const
{
    return mSegmentData->GetSegmentId();
}

BuildingSegmentReaderPtr TableWriter::CreateBuildingSegmentReader(
    const util::UnsafeSimpleMemoryQuotaControllerPtr& memoryController)
{
    return BuildingSegmentReaderPtr();
}

IE_NAMESPACE_END(table);

