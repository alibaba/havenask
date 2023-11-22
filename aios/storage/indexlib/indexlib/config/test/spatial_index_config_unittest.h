#pragma once

#include "autil/Log.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

class SpatialIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    SpatialIndexConfigTest();
    ~SpatialIndexConfigTest();

    DECLARE_CLASS_NAME(SpatialIndexConfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestDefault();
    void TestNotSupportEnableNull();

private:
    IndexPartitionSchemaPtr mSchema;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SpatialIndexConfigTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexConfigTest, TestDefault);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexConfigTest, TestNotSupportEnableNull);
}} // namespace indexlib::config
