#ifndef __INDEXLIB_TABLE_WRITER_H
#define __INDEXLIB_TABLE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/document.h"
#include "indexlib/table/table_common.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"
#include <fslib/fslib.h>

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, SegmentDataBase);
DECLARE_REFERENCE_CLASS(table, TableResource);
DECLARE_REFERENCE_CLASS(table, BuildingSegmentReader);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(table);

class TableWriter
{
public:
    enum class BuildResult {
        BR_OK = 0, BR_SKIP = 1, BR_FAIL = 2, BR_NO_SPACE = 3, BR_FATAL = 4, BR_UNKNOWN = -1
    };
public:
    TableWriter();
    virtual ~TableWriter();
public:
    bool Init(const TableWriterInitParamPtr& initParam);

    virtual bool DoInit() = 0; 
    virtual BuildResult Build(docid_t docId, const document::DocumentPtr& doc) = 0;
    virtual bool IsDirty() const = 0;
    virtual bool DumpSegment(BuildSegmentDescription& segmentDescription) = 0;
    virtual BuildingSegmentReaderPtr CreateBuildingSegmentReader(
        const util::UnsafeSimpleMemoryQuotaControllerPtr& memoryController);
    virtual bool IsFull() const;
    virtual size_t GetLastConsumedMessageCount() const = 0;

public:
    segmentid_t GetBuildingSegmentId() const;
    BuildingSegmentReaderPtr GetBuildingSegmentReader() const { return mBuildingSegmentReader; }
    void UpdateMemoryUse();
    index_base::Version GetOnDiskVersion() { return mOnDiskVersion; }

public:
    virtual size_t EstimateInitMemoryUse(
        const TableWriterInitParamPtr& initParam) const = 0;
    virtual size_t GetCurrentMemoryUse() const = 0;
    virtual size_t EstimateDumpMemoryUse() const = 0;
    virtual size_t EstimateDumpFileSize() const = 0;
protected:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

protected:
    file_system::DirectoryPtr mRootDirectory;
    file_system::DirectoryPtr mSegmentDataDirectory;
    index_base::SegmentDataBasePtr mSegmentData;
    index_base::Version mOnDiskVersion;
    TableResourcePtr mTableResource;
    PartitionRange mPartitionRange;
    
private:
    BuildingSegmentReaderPtr mBuildingSegmentReader;
    util::UnsafeSimpleMemoryQuotaControllerPtr mMemoryController;

    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableWriter);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_WRITER_H

