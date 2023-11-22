#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/ExceptionTrigger.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/test/fake_index_partition_reader_base.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

namespace indexlib { namespace partition {

class FakeOnlinePartition : public OnlinePartition
{
public:
    FakeOnlinePartition(const config::IndexPartitionSchemaPtr& schema, const IndexPartitionReaderPtr& reader)
        : mLeftQuota(0)
        , mThrowException(false)
    {
        mSchema = schema;
        mIndexReader = reader;
        mReaderUpdater.reset(new SearchReaderUpdater(schema->GetSchemaName()));
    }
    ~FakeOnlinePartition() {}

public:
    static partition::IndexPartitionPtr Create(const config::IndexPartitionSchemaPtr& schema,
                                               const IndexPartitionReaderPtr& reader)
    {
        auto fakeReader = DYNAMIC_POINTER_CAST(FakeIndexPartitionReaderBase, reader);
        fakeReader->SetSchema(schema);
        return partition::IndexPartitionPtr(new FakeOnlinePartition(schema, reader));
    }
    OpenStatus Open(const std::string& dir, const std::string& secondaryDir,
                    const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                    versionid_t version = INVALID_VERSIONID) override
    {
        // TODO(panghai.hj): Try all UT to see if static is needed here.
        util::MemoryQuotaControllerPtr memoryQuotaController(new util::MemoryQuotaController(10000000));
        mPartitionMemController.reset(new util::PartitionMemoryQuotaController(memoryQuotaController, mPartitionName));
        mMemController.reset(new util::BlockMemoryQuotaController(mPartitionMemController, "file_system"));
        return OS_OK;
    }
    OpenStatus ReOpen(bool forceReopen, versionid_t reopenVersionId = INVALID_VERSIONID) override { return OS_OK; }
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
    partition::PartitionWriterPtr GetWriter() const override { return partition::PartitionWriterPtr(); }
    IndexPartitionReaderPtr GetReader() const override { return mIndexReader; }
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
    void SetSchema(const config::IndexPartitionSchemaPtr& schema) { mSchema = schema; }
    void SetReader(const IndexPartitionReaderPtr& reader) { mIndexReader = reader; }
    void SetLeftQuota(size_t leftQuota) { mLeftQuota = leftQuota; }
    void SetThrowException(bool throwException) { mThrowException = throwException; }
    SearchReaderUpdaterPtr GetReaderUpdater() { return mReaderUpdater; }

protected:
    IndexPartitionReaderPtr mIndexReader;
    util::BlockMemoryQuotaControllerPtr mMemController;
    size_t mLeftQuota;
    bool mThrowException;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeOnlinePartition);
}} // namespace indexlib::partition
