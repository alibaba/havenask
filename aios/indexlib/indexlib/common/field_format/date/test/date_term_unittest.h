#ifndef __INDEXLIB_DATETERMTEST_H
#define __INDEXLIB_DATETERMTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/date/test/date_term_maker.h"

IE_NAMESPACE_BEGIN(common);

class DateTermTest : public INDEXLIB_TESTBASE
{
public:
    DateTermTest();
    ~DateTermTest();

    DECLARE_CLASS_NAME(DateTermTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCalculateTerms();
public:

private:
    std::pair<uint64_t, uint64_t> ParseTime(uint64_t left, uint64_t right,
        config::DateLevelFormat format);    

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DateTermTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DateTermTest, TestCalculateTerms);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DATETERMTEST_H
