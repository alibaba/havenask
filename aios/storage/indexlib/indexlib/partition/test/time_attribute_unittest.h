#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class TimeAttributeTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    TimeAttributeTest();
    ~TimeAttributeTest();

    DECLARE_CLASS_NAME(TimeAttributeTest);

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}

    void TestTimeField();
    void TestTimestampField();
    void TestDateField();
    void TestNullTimeField();
    void TestNullTimestampField();
    void TestNullDateField();
    void TestTimestampFieldWithDefaultTimeZoneDelta();

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, TimeAttributeTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeAttributeTest, TestTimeField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeAttributeTest, TestTimestampField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeAttributeTest, TestDateField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeAttributeTest, TestNullTimeField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeAttributeTest, TestNullTimestampField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeAttributeTest, TestNullDateField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeAttributeTest, TestTimestampFieldWithDefaultTimeZoneDelta);
}} // namespace indexlib::partition
