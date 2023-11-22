#pragma once

#include "autil/Log.h"
#include "indexlib/config/value_config.h"
#include "indexlib/util/testutil/unittest.h"

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
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ValueConfigTest, TestGetActualFieldType);
}} // namespace indexlib::config
