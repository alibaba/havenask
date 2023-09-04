#pragma once

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/partition_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class MockPartitionWriter : public partition::PartitionWriter
{
public:
    MockPartitionWriter(const config::IndexPartitionOptions& options) : PartitionWriter(options) {}
    ~MockPartitionWriter() {}

public:
    MOCK_METHOD(void, Open,
                (const config::IndexPartitionSchemaPtr&, const index_base::PartitionDataPtr&,
                 const PartitionModifierPtr&),
                (override));
    MOCK_METHOD(void, ReOpenNewSegment, (const PartitionModifierPtr&), (override));
    MOCK_METHOD(bool, BuildDocument, (const document::DocumentPtr&), (override));
    MOCK_METHOD(bool, callBatchBuild, (document::DocumentCollector*), ());
    bool BatchBuild(std::unique_ptr<document::DocumentCollector> docCollector) override
    {
        return callBatchBuild(docCollector.get());
    }

    MOCK_METHOD(bool, NeedDump, (size_t, const document::DocumentCollector*), (const, override));
    MOCK_METHOD(void, DumpSegment, (), (override));
    MOCK_METHOD(void, Close, (), (override));
    MOCK_METHOD(void, RewriteDocument, (const document::DocumentPtr&), (override));
    MOCK_METHOD(uint64_t, GetTotalMemoryUse, (), (const, override));
    MOCK_METHOD(void, ReportMetrics, (), (const, override));
    MOCK_METHOD(void, SetEnableReleaseOperationAfterDump, (bool), (override));
};

DEFINE_SHARED_PTR(MockPartitionWriter);
}} // namespace indexlib::partition
