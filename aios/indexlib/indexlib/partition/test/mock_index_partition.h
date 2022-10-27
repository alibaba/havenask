#ifndef __INDEXLIB_MOCK_INDEX_PARTITION_H
#define __INDEXLIB_MOCK_INDEX_PARTITION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/storage/exception_trigger.h"
#include "indexlib/partition/test/fake_index_partition_reader_base.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/partition/index_partition_resource.h"

IE_NAMESPACE_BEGIN(partition);

class MockIndexPartition : public IndexPartition
{
public:
    MockIndexPartition()
    {
    }
    ~MockIndexPartition()
    {}
public:
    virtual OpenStatus Open(const std::string& primaryDir,
                            const std::string& secondaryDir,
                            const config::IndexPartitionSchemaPtr& schema, 
                            const config::IndexPartitionOptions& options,
                            versionid_t version = INVALID_VERSION)
    {
        return OS_OK;
    }
    virtual OpenStatus ReOpen(bool forceReopen, 
                              versionid_t reopenVersionId = INVALID_VERSION)
    {
        return OS_OK;
    }
    virtual void ReOpenNewSegment()
    {}
    virtual void Close()
    {}
    virtual partition::PartitionWriterPtr GetWriter() const
    {
        return partition::PartitionWriterPtr();
    }
    virtual IndexPartitionReaderPtr GetReader() const
    {
        return mReader;
    }


public:
    void SetSchema(const config::IndexPartitionSchemaPtr &schema)
    {
        mSchema = schema;
    }
    void SetReader(const IndexPartitionReaderPtr &reader)
    {
        mReader = reader;
    }

protected:
    IndexPartitionReaderPtr mReader;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MockIndexPartition);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MOCK_INDEX_PARTITION_H
