#ifndef __INDEXLIB_RANGEINDEXCONFIGTEST_H
#define __INDEXLIB_RANGEINDEXCONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/config/impl/range_index_config_impl.h"

IE_NAMESPACE_BEGIN(config);

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
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RangeIndexConfigTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(RangeIndexConfigTest, TestConfigError);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_RANGEINDEXCONFIGTEST_H
