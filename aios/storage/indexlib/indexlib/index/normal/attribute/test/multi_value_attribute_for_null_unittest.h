#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class MultiValueAttributeForNullTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    MultiValueAttributeForNullTest();
    ~MultiValueAttributeForNullTest();

    DECLARE_CLASS_NAME(MultiValueAttributeForNullTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMultiValueForNull_1();
    void TestMultiValueForNull_2();
    void TestMultiValueForNull_3();
    void TestMultiValueForNull_4();

private:
    void InnerTestMultiValueForNull(const std::string& fieldType, const std::string& compressType, bool packed);

    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiValueAttributeForNullTest, TestMultiValueForNull_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiValueAttributeForNullTest, TestMultiValueForNull_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiValueAttributeForNullTest, TestMultiValueForNull_3);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiValueAttributeForNullTest, TestMultiValueForNull_4);
INSTANTIATE_TEST_CASE_P(BuildMode, MultiValueAttributeForNullTest, testing::Values(0, 1, 2));
}} // namespace indexlib::index
