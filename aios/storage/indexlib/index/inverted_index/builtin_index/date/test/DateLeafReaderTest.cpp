#include "indexlib/index/inverted_index/builtin_index/date/DateLeafReader.h"

#include "indexlib/index/inverted_index/builtin_index/date/TimeRangeInfo.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DateLeafReaderTest : public TESTBASE
{
public:
    DateLeafReaderTest();
    ~DateLeafReaderTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, DateLeafReaderTest);

DateLeafReaderTest::DateLeafReaderTest() {}

DateLeafReaderTest::~DateLeafReaderTest() {}

TEST_F(DateLeafReaderTest, TestNormalizeTimestamp)
{
    auto config = std::make_shared<indexlibv2::config::DateIndexConfig>("", it_date, "");
    DateLeafReader reader(config, IndexFormatOption(), 0, nullptr, nullptr, TimeRangeInfo());
    {
        uint64_t leftTerm = DateTerm(2010, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t rightTerm = DateTerm(2012, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t minTime = DateTerm(2011, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t maxTime = DateTerm(2011, 5, 3, 12, 55, 30, 11).GetKey();
        reader.NormalizeTerms(minTime, maxTime, leftTerm, rightTerm);
        uint64_t expectLeftTerm = DateTerm(2011, 0, 0, 0, 0, 0, 0).GetKey();
        uint64_t expectRightTerm = DateTerm(2012, 0, 0, 0, 0, 0, 0).GetKey();
        ASSERT_EQ(expectLeftTerm, leftTerm);
        ASSERT_EQ(expectRightTerm, rightTerm);
    }

    {
        uint64_t leftTerm = DateTerm(2010, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t rightTerm = DateTerm(2012, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t minTime = DateTerm(2010, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t maxTime = DateTerm(2012, 2, 3, 12, 55, 30, 11).GetKey();
        reader.NormalizeTerms(minTime, maxTime, leftTerm, rightTerm);
        uint64_t expectLeftTerm = DateTerm(2010, 0, 0, 0, 0, 0, 0).GetKey();
        uint64_t expectRightTerm = DateTerm(2013, 0, 0, 0, 0, 0, 0).GetKey();
        ASSERT_EQ(expectLeftTerm, leftTerm);
        ASSERT_EQ(expectRightTerm, rightTerm);
    }

    {
        uint64_t leftTerm = DateTerm(2010, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t rightTerm = DateTerm(2012, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t minTime = DateTerm(2010, 1, 3, 12, 55, 30, 11).GetKey();
        uint64_t maxTime = DateTerm(2013, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t expectLeftTerm = leftTerm;
        uint64_t expectRightTerm = rightTerm;
        reader.NormalizeTerms(minTime, maxTime, leftTerm, rightTerm);
        ASSERT_EQ(expectLeftTerm, leftTerm);
        ASSERT_EQ(expectRightTerm, rightTerm);
    }
}

} // namespace indexlib::index
