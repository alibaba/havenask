#pragma once

#include "autil/Log.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

class RangeIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    RangeIndexConfigTest();
    ~RangeIndexConfigTest();

    DECLARE_CLASS_NAME(RangeIndexConfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestConfigError();

private:
    IndexPartitionSchemaPtr mSchema;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RangeIndexConfigTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(RangeIndexConfigTest, TestConfigError);
}} // namespace indexlib::config
