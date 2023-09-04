#include "indexlib/index/common/field_format/date/DateQueryParser.h"

#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/common/field_format/date/test/DateTermMaker.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DateQueryParserTest : public TESTBASE
{
public:
    DateQueryParserTest();
    ~DateQueryParserTest();

public:
    void setUp() override;
    void TestParseQuery();

private:
    void InnerTestParseQuery(config::DateLevelFormat::Granularity granularity, int64_t leftTerm, int64_t rightTerm,
                             bool parseSuccess, uint64_t expectLeftTerm, uint64_t expectRightTerm);

private:
    std::shared_ptr<indexlibv2::config::DateIndexConfig> _dateConfig;
};

TEST_F(DateQueryParserTest, TestParseQuery) { TestParseQuery(); }

DateQueryParserTest::DateQueryParserTest() {}

DateQueryParserTest::~DateQueryParserTest() {}

void DateQueryParserTest::setUp()
{
    std::string field = "time:uint64;";
    std::string index = "time_index:date:time;";
    std::string attribute = "time";

    auto schema = indexlibv2::table::NormalTabletSchemaMaker::Make(field, index, attribute, "");
    ASSERT_TRUE(schema);
    _dateConfig = std::dynamic_pointer_cast<indexlibv2::config::DateIndexConfig>(
        schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, "time_index"));
    ASSERT_TRUE(_dateConfig);
}

void DateQueryParserTest::InnerTestParseQuery(config::DateLevelFormat::Granularity granularity, int64_t left,
                                              int64_t right, bool parseSuccess, uint64_t expectLeftTerm,
                                              uint64_t expectRightTerm)
{
    tearDown();
    setUp();
    config::DateLevelFormat format;
    format.Init(granularity);
    _dateConfig->SetBuildGranularity(granularity);
    _dateConfig->SetDateLevelFormat(format);
    index::Int64Term term(left, true, right, true, "time_index");
    DateQueryParser parser;
    parser.Init(_dateConfig->GetBuildGranularity(), _dateConfig->GetDateLevelFormat());
    uint64_t leftTerm, rightTerm;
    ASSERT_EQ(parseSuccess, parser.ParseQuery(&term, leftTerm, rightTerm));
    if (parseSuccess) {
        ASSERT_EQ(expectLeftTerm, leftTerm);
        ASSERT_EQ(expectRightTerm, rightTerm);
    }
}

void DateQueryParserTest::TestParseQuery()
{
    // 599616000000:1989-01-01 00:00:00; 915148800000:1999-01-01 00:00:00
    InnerTestParseQuery(config::DateLevelFormat::YEAR, 599616000001, 915148800001, true,
                        DateTerm(1990 - 1970, 0, 0, 0, 0, 0, 0).GetKey(),
                        DateTerm(1999 - 1970, 0, 0, 0, 0, 0, 0).GetKey());

    // test invalid input
    InnerTestParseQuery(config::DateLevelFormat::YEAR, -1, -2, false, 0, 0);
    InnerTestParseQuery(config::DateLevelFormat::YEAR, 5, 3, false, 0, 0);
    // test left 0
    InnerTestParseQuery(config::DateLevelFormat::YEAR, 0, 599616000000, true, DateTerm(0, 0, 0, 0, 0, 0, 0).GetKey(),
                        DateTerm(1989 - 1970, 0, 0, 0, 0, 0, 0).GetKey());

    InnerTestParseQuery(config::DateLevelFormat::YEAR, 0, 0, true, DateTerm(0, 0, 0, 0, 0, 0, 0).GetKey(),
                        DateTerm(0, 0, 0, 0, 0, 0, 0).GetKey());
}

} // namespace indexlib::index
