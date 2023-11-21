#pragma once

#include "autil/Log.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

class DateIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    DateIndexConfigTest();
    ~DateIndexConfigTest();

    DECLARE_CLASS_NAME(DateIndexConfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestConfigError();

private:
    void InnerTestGranularity(const std::string& indexName, DateLevelFormat::Granularity expectedGranularity);

private:
    IndexPartitionSchemaPtr mSchema;
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DateIndexConfigTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DateIndexConfigTest, TestConfigError);
}} // namespace indexlib::config
