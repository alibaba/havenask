#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/ExceptionTrigger.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/partition_writer.h"
#include "indexlib/partition/test/fake_index_partition_reader_base.h"
#include "indexlib/partition/test/mock_partition_writer.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"

namespace indexlib { namespace partition {

class MockIndexPartition : public IndexPartition
{
public:
    MockIndexPartition(const IndexPartitionResource& partitionResource) : IndexPartition(partitionResource)
    {
        mWriter = MockPartitionWriterPtr(new MockPartitionWriter(mOptions));
    }
    MockIndexPartition(const IndexPartitionResource& partitionResource, partition::PartitionWriterPtr writer)
        : IndexPartition(partitionResource)
    {
        mWriter = writer;
    }
    ~MockIndexPartition() {}

public:
    virtual OpenStatus Open(const std::string& primaryDir, const std::string& secondaryDir,
                            const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                            versionid_t version = INVALID_VERSIONID)
    {
        return OS_OK;
    }
    virtual OpenStatus ReOpen(bool forceReopen, versionid_t reopenVersionId = INVALID_VERSIONID) { return OS_OK; }
    virtual void ReOpenNewSegment() {}
    virtual void Close() {}
    virtual partition::PartitionWriterPtr GetWriter() const { return mWriter; }
    virtual IndexPartitionReaderPtr GetReader() const { return mReader; }

public:
    void SetSchema(const config::IndexPartitionSchemaPtr& schema) { mSchema = schema; }
    void SetOptions(const config::IndexPartitionOptions& options) { mOptions = options; }
    void SetReader(const IndexPartitionReaderPtr& reader) { mReader = reader; }
    void SetPartitionData(const file_system::IFileSystemPtr& fileSystem)
    {
        mPartitionDataHolder.Reset(test::PartitionDataMaker::CreateSimplePartitionData(fileSystem));
        mRootDirectory = file_system::Directory::Get(fileSystem);
    }

public:
    static IndexPartitionResource CreatePartitionResource()
    {
        IndexPartitionResource partitionResource;
        partitionResource.taskScheduler = util::TaskSchedulerPtr(new util::TaskScheduler);
        partitionResource.memoryQuotaController =
            std::make_shared<util::MemoryQuotaController>(/*totalQuota=*/1024 * 1024 * 1024);
        return partitionResource;
    }

protected:
    bool NotAllowToModifyRootPath() const { return true; };
    void CheckParam(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options) const
    {
    }

    PartitionWriterPtr mWriter;
    IndexPartitionReaderPtr mReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MockIndexPartition);
}} // namespace indexlib::partition
