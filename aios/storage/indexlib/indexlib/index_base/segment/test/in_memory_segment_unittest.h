#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class InMemorySegmentTest : public INDEXLIB_TESTBASE
{
public:
    InMemorySegmentTest();
    ~InMemorySegmentTest();

    DECLARE_CLASS_NAME(InMemorySegmentTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDump();
    void TestDumpForSub();

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemorySegmentTest, TestDump);
INDEXLIB_UNIT_TEST_CASE(InMemorySegmentTest, TestDumpForSub);
}} // namespace indexlib::index_base
