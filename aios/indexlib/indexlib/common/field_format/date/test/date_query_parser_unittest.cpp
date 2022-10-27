#include "indexlib/common/field_format/date/test/date_query_parser_unittest.h"
#include "indexlib/common/field_format/date/test/date_term_maker.h"
#include "indexlib/common/number_term.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(date, DateQueryParserTest);

DateQueryParserTest::DateQueryParserTest()
{
}

DateQueryParserTest::~DateQueryParserTest()
{
}

void DateQueryParserTest::CaseSetUp()
{
    string field = "time:uint64;";
    string index = "time_index:date:time;";
    string attribute = "time";
    
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    mDateConfig = DYNAMIC_POINTER_CAST(
            DateIndexConfig, indexSchema->GetIndexConfig("time_index"));
}

void DateQueryParserTest::CaseTearDown()
{
}

void DateQueryParserTest::InnerTestParseQuery(
        DateLevelFormat::Granularity granularity,
        int64_t left, int64_t right, bool parseSuccess,
        uint64_t expectLeftTerm, uint64_t expectRightTerm)
{
    TearDown();
    SetUp();
    DateLevelFormat format;
    format.Init(granularity);
    mDateConfig->SetBuildGranularity(granularity);
    mDateConfig->SetDateLevelFormat(format);
    common::Int64Term term(left, true, right, true, "time_index");
    DateQueryParser parser;
    parser.Init(mDateConfig);
    uint64_t leftTerm,rightTerm;
    ASSERT_EQ(parseSuccess, parser.ParseQuery(term, leftTerm, rightTerm));
    if (parseSuccess)
    {
        ASSERT_EQ(expectLeftTerm, leftTerm);
        ASSERT_EQ(expectRightTerm, rightTerm);
    }
}

void DateQueryParserTest::TestParseQuery()
{
    //599616000000:1989-01-01 00:00:00; 915148800000:1999-01-01 00:00:00
    InnerTestParseQuery(DateLevelFormat::YEAR, 599616000001,
                        915148800001, true,
                        DateTerm(1990 - 1970, 0, 0, 0, 0, 0, 0).GetKey(),
                        DateTerm(1999 - 1970, 0, 0, 0, 0, 0, 0).GetKey());

    //test invalid input
    InnerTestParseQuery(DateLevelFormat::YEAR, -1, -2, false, 0, 0);
    InnerTestParseQuery(DateLevelFormat::YEAR, 5, 3, false, 0, 0);
    //test left 0
    InnerTestParseQuery(DateLevelFormat::YEAR, 0, 599616000000, true,
                        DateTerm(0, 0, 0, 0, 0, 0, 0).GetKey(),
                        DateTerm(1989 - 1970, 0, 0, 0, 0, 0, 0).GetKey());

    InnerTestParseQuery(DateLevelFormat::YEAR, 0, 0, true,
                        DateTerm(0, 0, 0, 0, 0, 0, 0).GetKey(),
                        DateTerm(0, 0, 0, 0, 0, 0, 0).GetKey());
}

IE_NAMESPACE_END(common);

