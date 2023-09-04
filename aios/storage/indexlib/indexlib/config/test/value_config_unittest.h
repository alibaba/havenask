#ifndef __INDEXLIB_VALUECONFIGTEST_H
#define __INDEXLIB_VALUECONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/value_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {

class ValueConfigTest : public INDEXLIB_TESTBASE
{
public:
    ValueConfigTest();
    ~ValueConfigTest();

    DECLARE_CLASS_NAME(ValueConfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetActualFieldType();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ValueConfigTest, TestGetActualFieldType);
}} // namespace indexlib::config

#endif //__INDEXLIB_VALUECONFIGTEST_H
