#ifndef __INDEXLIB_SPATIALINDEXCONFIGTEST_H
#define __INDEXLIB_SPATIALINDEXCONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

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
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SpatialIndexConfigTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexConfigTest, TestDefault);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexConfigTest, TestNotSupportEnableNull);
}} // namespace indexlib::config

#endif //__INDEXLIB_SPATIALINDEXCONFIGTEST_H
