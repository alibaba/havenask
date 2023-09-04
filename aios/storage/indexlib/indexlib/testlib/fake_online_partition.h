#ifndef __INDEXLIB_FAKE_ONLINE_PARTITION_H
#define __INDEXLIB_FAKE_ONLINE_PARTITION_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/ExceptionTrigger.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/testlib/fake_index_partition_writer.h"

namespace indexlib { namespace testlib {

class FakeOnlinePartition : public partition::OnlinePartition
{
public:
    FakeOnlinePartition(const config::IndexPartitionSchemaPtr& schema, const partition::IndexPartitionReaderPtr& reader)
        : mWriter(new index::FakeIndexPartitionWriter(mOptions))
        , mLeftQuota(0)
        , mThrowException(false)
        , mGetReaderTimes(0)
    {
        mSchema = schema;
        mIndexReader = reader;
    }
    FakeOnlinePartition()
        : mWriter(new index::FakeIndexPartitionWriter(mOptions))
        , mLeftQuota(0)
        , mThrowException(false)
        , mGetReaderTimes(0)
    {
    }
    ~FakeOnlinePartition() {}

public:
    static partition::IndexPartitionPtr Create(const config::IndexPartitionSchemaPtr& schema,
                                               const partition::IndexPartitionReaderPtr& reader)
    {
        auto fakeReader = DYNAMIC_POINTER_CAST(FakeIndexPartitionReaderBase, reader);
        fakeReader->SetSchema(schema);
        return partition::IndexPartitionPtr(new FakeOnlinePartition(schema, reader));
    }
    OpenStatus Open(const std::string& dir, const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options, versionid_t version = INVALID_VERSION) override
    {
        static util::MemoryQuotaControllerPtr memoryQuotaController(new util::MemoryQuotaController(10000000));
        mMemQuotaController = memoryQuotaController;
        mPartitionMemController.reset(new util::PartitionMemoryQuotaController(mMemQuotaController, mPartitionName));
        mMemController.reset(new util::BlockMemoryQuotaController(mPartitionMemController, "file_system"));
        return OS_OK;
    }
    OpenStatus ReOpen(bool forceReopen, versionid_t reopenVersionId = INVALID_VERSION) override { return OS_OK; }
    void ReOpenNewSegment() override {}
    void AddVirtualAttributes(const config::AttributeConfigVector& mainVirtualAttrConfigs,
                              const config::AttributeConfigVector& subVirtualAttrConfigs) override
    {
        if (mThrowException) {
            INDEXLIB_THROW(util::FileIOException, "file exception.");
        }
        versionid_t vid = mIndexReader->GetIncVersionId();
        mIndexReader.reset(new FakeIndexPartitionReaderBase(mSchema, vid));
    }
    void Close() override {}
    partition::PartitionWriterPtr GetWriter() const override { return mWriter; }
    partition::IndexPartitionReaderPtr GetReader() const override
    {
        ++mGetReaderTimes;
        return mIndexReader;
    }
    util::MemoryReserverPtr
    ReserveVirtualAttributesResource(const config::AttributeConfigVector& mainVirtualAttrConfigs,
                                     const config::AttributeConfigVector& subVirtualAttrConfigs) override
    {
        if ((mainVirtualAttrConfigs.empty() && subVirtualAttrConfigs.empty()) || mLeftQuota) {
            return util::MemoryReserverPtr(new util::MemoryReserver("virtual_attribute", mMemController));
        }
        return util::MemoryReserverPtr();
    }

public:
    size_t getReaderTimes() const { return mGetReaderTimes; }
    void setSchema(const std::string& root, const std::string& schemaFileName)
    {
        mSchema = index_base::SchemaAdapter::LoadSchema(root, schemaFileName);
    }
    void SetSchema(const config::IndexPartitionSchemaPtr& schema) { mSchema = schema; }
    void SetReader(const partition::IndexPartitionReaderPtr& reader) { mIndexReader = reader; }
    void SetLeftQuota(size_t leftQuota) { mLeftQuota = leftQuota; }
    void SetThrowException(bool throwException) { mThrowException = throwException; }

protected:
    partition::IndexPartitionReaderPtr mIndexReader;
    util::BlockMemoryQuotaControllerPtr mMemController;
    partition::PartitionWriterPtr mWriter;
    size_t mLeftQuota;
    bool mThrowException;
    mutable size_t mGetReaderTimes;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeOnlinePartition);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_MOCK_INDEX_PARTITION_H
