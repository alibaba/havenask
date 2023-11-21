#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class CustomTableTest : public INDEXLIB_TESTBASE
{
public:
    CustomTableTest();
    ~CustomTableTest();

    DECLARE_CLASS_NAME(CustomTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomTableTest, TestSimpleProcess);
}} // namespace indexlib::partition
