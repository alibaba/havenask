#include "indexlib/index/common/field_format/date/DateFieldEncoder.h"
#include "indexlib/index/common/field_format/date/test/DateTermMaker.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DateTermTest : public TESTBASE
{
public:
    DateTermTest();
    ~DateTermTest();

public:
    void TestSimpleProcess();
    void TestCalculateTerms();

private:
    std::pair<uint64_t, uint64_t> ParseTime(uint64_t left, uint64_t right, config::DateLevelFormat format);
};

TEST_F(DateTermTest, TestSimpleProcess) { TestSimpleProcess(); }
TEST_F(DateTermTest, TestCalculateTerms) { TestCalculateTerms(); }

DateTermTest::DateTermTest() {}

DateTermTest::~DateTermTest() {}

std::pair<uint64_t, uint64_t> DateTermTest::ParseTime(uint64_t left, uint64_t right, config::DateLevelFormat format)
{
    util::TimestampUtil::TM leftDate = util::TimestampUtil::ConvertToTM(left);
    util::TimestampUtil::TM rightDate = util::TimestampUtil::ConvertToTM(right);
    DateTerm leftDateTerm = DateTerm::ConvertToDateTerm(leftDate, format);
    DateTerm rightDateTerm = DateTerm::ConvertToDateTerm(rightDate, format);
    return std::make_pair(leftDateTerm.GetKey(), rightDateTerm.GetKey());
}

void DateTermTest::TestSimpleProcess()
{
    uint64_t leftDateTerm, rightDateTerm;
    {
        config::DateLevelFormat format;
        format.Init(3415, config::DateLevelFormat::MILLISECOND); // no middle level
        // from 2014-11-22 00:05:12 to 2014-11-22 02:01:52
        std::tie(leftDateTerm, rightDateTerm) = ParseTime(1416614712000, 1416621712000, format);
        std::vector<uint64_t> terms;
        DateTerm::CalculateTerms(leftDateTerm, rightDateTerm, format, terms);
        std::vector<uint64_t> expectTerms;
        DateTermMaker::MakeDateTerms(44, 11, 22, 0, 5, 12, 59, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 0, 6, 59, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 2, 1, 52, 0, 0, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 2, 1, 0, 51, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 2, 0, 0, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 1, 1, expectTerms);
        std::sort(expectTerms.begin(), expectTerms.end());
        std::sort(terms.begin(), terms.end());
        ASSERT_EQ(expectTerms.size(), terms.size());
        ASSERT_EQ(expectTerms, terms);
    }
    {
        config::DateLevelFormat format;
        format.Init(3415, config::DateLevelFormat::SECOND); // no middle level
        // from 2014-11-22 00:05:12 to 2016-07-27 12:15:11
        std::tie(leftDateTerm, rightDateTerm) = ParseTime(1416614712000, 1469621711000, format);
        std::vector<uint64_t> terms;
        DateTerm::CalculateTerms(leftDateTerm, rightDateTerm, format, terms);
        std::vector<uint64_t> expectTerms;
        DateTermMaker::MakeDateTerms(44, 11, 22, 0, 5, 12, 59, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 0, 6, 59, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 1, 23, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 23, 31, expectTerms);
        DateTermMaker::MakeDateTerms(44, 12, 12, expectTerms);
        DateTermMaker::MakeDateTerms(46, 7, 27, 12, 15, 0, 11, expectTerms);
        DateTermMaker::MakeDateTerms(46, 7, 27, 12, 0, 14, expectTerms);
        DateTermMaker::MakeDateTerms(46, 7, 27, 0, 11, expectTerms);
        DateTermMaker::MakeDateTerms(46, 7, 0, 26, expectTerms);
        DateTermMaker::MakeDateTerms(46, 0, 6, expectTerms);
        DateTermMaker::MakeDateTerms(45, 45, expectTerms);
        std::sort(expectTerms.begin(), expectTerms.end());
        std::sort(terms.begin(), terms.end());
        ASSERT_EQ(expectTerms.size(), terms.size());
        ASSERT_EQ(expectTerms, terms);
    }
}

void DateTermTest::TestCalculateTerms()
{
    uint64_t leftDateTerm, rightDateTerm;
    config::DateLevelFormat format;
    format.Init(3415, config::DateLevelFormat::SECOND);
    {
        // from 2014-11-22 00:05:59 to 2014-11-22 02:02:00
        //     [2014-11-22 00:06:00 ,  2014-11-22 02:01:59]
        //     "(1416614759000,1416621720000)";
        std::vector<uint64_t> expectTerms, terms;
        DateTermMaker::MakeDateTerms(44, 11, 22, 0, 6, 59, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 2, 0, 1, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 1, 1, expectTerms);
        std::tie(leftDateTerm, rightDateTerm) = ParseTime(1416614760000LL, 1416621719000LL, format);
        DateTerm::CalculateTerms(leftDateTerm, rightDateTerm, format, terms);
        std::sort(terms.begin(), terms.end());
        std::sort(expectTerms.begin(), expectTerms.end());
        // ASSERT_EQ(expectTerms.size(), terms.size());
        ASSERT_EQ(expectTerms, terms);
    }
    {
        // from 2014-11-22 00:05:59 to 2014-11-22 02:02:00
        // "(1416614759000,*)";
        std::vector<uint64_t> expectTerms, terms;
        DateTermMaker::MakeDateTerms(44, 11, 22, 0, 6, 59, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 2, 0, 1, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 2, 2, 0, 0, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 22, 1, 1, expectTerms);
        std::sort(expectTerms.begin(), expectTerms.end());
        std::tie(leftDateTerm, rightDateTerm) = ParseTime(1416614760000LL, 1416621720000LL, format);
        DateTerm::CalculateTerms(leftDateTerm, rightDateTerm, format, terms);
        std::sort(terms.begin(), terms.end());
        // ASSERT_EQ(expectTerms.size(), terms.size());
        ASSERT_EQ(expectTerms, terms);
    }
    {
        format.Init(3415, config::DateLevelFormat::MINUTE);
        // from 2018-01-16 04:19:00 to 2018-01-16 09:52:00
        // "[1516076340000, 1516096320000]";
        std::vector<uint64_t> expectTerms, terms;
        DateTermMaker::MakeDateTerms(48, 1, 16, 4, 19, 59, expectTerms);
        DateTermMaker::MakeDateTerms(48, 1, 16, 5, 8, expectTerms);
        DateTermMaker::MakeDateTerms(48, 1, 16, 9, 0, 52, expectTerms);
        std::sort(expectTerms.begin(), expectTerms.end());
        std::tie(leftDateTerm, rightDateTerm) = ParseTime(1516076340000LL, 1516096320000LL, format);
        DateTerm::CalculateTerms(leftDateTerm, rightDateTerm, format, terms);
        std::sort(terms.begin(), terms.end());
        ASSERT_EQ(expectTerms, terms);
    }
    {
        format.Init(3415, config::DateLevelFormat::DAY);
        // from 2017-8-31 00:00:00 to 2018-01-1 00:00:00
        // "[1504137600000,1514764800000)";
        std::vector<uint64_t> expectTerms, terms;
        DateTermMaker::MakeDateTerms(47, 8, 31, 31, expectTerms);
        DateTermMaker::MakeDateTerms(47, 9, 12, expectTerms);
        std::tie(leftDateTerm, rightDateTerm) = ParseTime(1504137600000LL, 1514678400000LL, format);
        DateTerm::CalculateTerms(leftDateTerm, rightDateTerm, format, terms);
        std::sort(terms.begin(), terms.end());
        std::sort(expectTerms.begin(), expectTerms.end());

        // ASSERT_EQ(expectTerms.size(), terms.size());
        ASSERT_EQ(expectTerms, terms);

        // from 2017-8-31 00:00:00 to 2017-12-31 00:00:00
        //"(1504137600000,1514678400000]";
        terms.clear();
        expectTerms.clear();
        DateTermMaker::MakeDateTerms(47, 9, 12, expectTerms);
        std::sort(expectTerms.begin(), expectTerms.end());
        std::sort(terms.begin(), terms.end());
        std::tie(leftDateTerm, rightDateTerm) = ParseTime(1504137600000LL, 1514678400000LL, format);
        leftDateTerm += 1LL << 27;
        DateTerm::CalculateTerms(leftDateTerm, rightDateTerm, format, terms);
        std::sort(terms.begin(), terms.end());
        ASSERT_EQ(expectTerms, terms);
    }
    {
        std::vector<uint64_t> expectTerms, terms;
        format.Init(3415, config::DateLevelFormat::DAY);
        // from 2017-8-31 00:00:00 to 2017-12-31 00:00:00
        // "[1504137600000,1514678400000]";
        DateTermMaker::MakeDateTerms(47, 8, 31, 31, expectTerms);
        DateTermMaker::MakeDateTerms(47, 9, 12, expectTerms);
        std::tie(leftDateTerm, rightDateTerm) = ParseTime(1504137600000LL, 1514678400000LL, format);
        DateTerm::CalculateTerms(leftDateTerm, rightDateTerm, format, terms);
        std::sort(terms.begin(), terms.end());
        std::sort(expectTerms.begin(), expectTerms.end());
        // ASSERT_EQ(expectTerms.size(), terms.size());
        ASSERT_EQ(expectTerms, terms);
    }
    {
        // 110,111,111,111
        format.Init(3583, config::DateLevelFormat::MILLISECOND);
        // from 2014-11-25 20:40:00 123ms to 2018-01-26 04:11:29]
        std::vector<uint64_t> expectTerms, terms;
        std::tie(leftDateTerm, rightDateTerm) = ParseTime(1416948000123LL, 1516939889000LL, format);
        DateTerm::CalculateTerms(leftDateTerm, rightDateTerm, format, terms);
        DateTermMaker::MakeDateTerms(44, 11, 25, 20, 40, 0, 123, 127, expectTerms);
        DateTermMaker::MakeMiddleTerms(44, 11, 25, 20, 40, 0, 4, 31, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 25, 20, 40, 1, 7, expectTerms);
        DateTermMaker::MakeMiddleTerms(44, 11, 25, 20, 40, 1, 7, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 25, 20, 41, 47, expectTerms);
        DateTermMaker::MakeMiddleTerms(44, 11, 25, 20, 6, 7, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 25, 21, 23, expectTerms);
        DateTermMaker::MakeDateTerms(44, 11, 26, 31, expectTerms);
        // DateTermMaker::MakeMiddleTerms(44, 11, 7, 7, expectTerms);
        DateTermMaker::MakeDateTerms(44, 12, 12, expectTerms);
        DateTermMaker::MakeDateTerms(48, 1, 26, 4, 11, 29, 0, 0, expectTerms);
        DateTermMaker::MakeDateTerms(48, 1, 26, 4, 11, 24, 28, expectTerms);
        DateTermMaker::MakeMiddleTerms(48, 1, 26, 4, 11, 0, 2, expectTerms);
        DateTermMaker::MakeDateTerms(48, 1, 26, 4, 8, 10, expectTerms);
        DateTermMaker::MakeMiddleTerms(48, 1, 26, 4, 0, 0, expectTerms);
        DateTermMaker::MakeMiddleTerms(48, 1, 26, 0, 0, expectTerms);
        DateTermMaker::MakeDateTerms(48, 1, 0, 25, expectTerms);
        DateTermMaker::MakeDateTerms(48, 0, 0, expectTerms);
        DateTermMaker::MakeDateTerms(45, 47, expectTerms);
        std::sort(expectTerms.begin(), expectTerms.end());
        std::sort(terms.begin(), terms.end());
        ASSERT_EQ(expectTerms.size(), terms.size());
        ASSERT_EQ(expectTerms, terms);
    }
    {
        std::vector<uint64_t> terms;
        std::tie(leftDateTerm, rightDateTerm) = ParseTime(1504137600000LL, 1500678400000LL, format);
        DateTerm::CalculateTerms(leftDateTerm, rightDateTerm, format, terms);
        ASSERT_TRUE(terms.empty());
    }
}

} // namespace indexlib::index
