#ifndef __INDEXLIB_DATEQUERYPARSERTEST_H
#define __INDEXLIB_DATEQUERYPARSERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/date/date_query_parser.h"

IE_NAMESPACE_BEGIN(common);

class DateQueryParserTest : public INDEXLIB_TESTBASE
{
public:
    DateQueryParserTest();
    ~DateQueryParserTest();

    DECLARE_CLASS_NAME(DateQueryParserTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestParseQuery();
private:
    void InnerTestParseQuery(
            config::DateLevelFormat::Granularity granularity,
            int64_t leftTerm, int64_t rightTerm, bool parseSuccess,
            uint64_t expectLeftTerm, uint64_t expectRightTerm);

private:
    config::DateIndexConfigPtr mDateConfig;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DateQueryParserTest, TestParseQuery);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DATEQUERYPARSERTEST_H
