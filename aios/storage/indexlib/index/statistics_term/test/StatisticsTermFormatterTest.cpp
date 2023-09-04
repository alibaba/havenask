#include "indexlib/index/statistics_term/StatisticsTermFormatter.h"

#include "unittest/unittest.h"

namespace indexlibv2::index {

class StatisticsTermFormatterTest : public TESTBASE
{
};

TEST_F(StatisticsTermFormatterTest, testGet)
{
    const std::string name = "ut";
    const size_t termCount = 100;
    ASSERT_EQ("ut\t100\n", StatisticsTermFormatter::GetHeaderLine(name, termCount));
    ASSERT_EQ("100\t2\tut\n", StatisticsTermFormatter::GetDataLine(name, termCount));
}

TEST_F(StatisticsTermFormatterTest, testParseHeaderLine)
{
    const std::string line = "ut\t100\n";
    size_t termCount;
    std::string name;
    ASSERT_TRUE(StatisticsTermFormatter::ParseHeaderLine(line, name, termCount).IsOK());
    ASSERT_EQ(100, termCount);
    ASSERT_EQ("ut", name);

    const std::string invalidLine = "ut100\n";
    ASSERT_FALSE(StatisticsTermFormatter::ParseHeaderLine(invalidLine, name, termCount).IsOK());
}

TEST_F(StatisticsTermFormatterTest, testParseDataLine)
{
    const std::string block = "100\t2\tut\n102\t3\tut1\n";
    size_t termHash;
    std::string term;
    size_t pos = 0;
    ASSERT_TRUE(StatisticsTermFormatter::ParseDataLine(block, pos, term, termHash).IsOK());
    ASSERT_EQ(100, termHash);
    ASSERT_EQ("ut", term);

    ASSERT_TRUE(StatisticsTermFormatter::ParseDataLine(block, pos, term, termHash).IsOK());
    ASSERT_EQ(102, termHash);
    ASSERT_EQ("ut1", term);

    const std::string invalidBlock1 = "100\t2\tut\n1023ut1\n";
    pos = 0;
    ASSERT_TRUE(StatisticsTermFormatter::ParseDataLine(invalidBlock1, pos, term, termHash).IsOK());
    ASSERT_EQ(100, termHash);
    ASSERT_EQ("ut", term);
    ASSERT_FALSE(StatisticsTermFormatter::ParseDataLine(invalidBlock1, pos, term, termHash).IsOK());

    const std::string invalidBlock2 = "100\t2\tut\n1023\tut1\n";
    pos = 0;
    ASSERT_TRUE(StatisticsTermFormatter::ParseDataLine(invalidBlock2, pos, term, termHash).IsOK());
    ASSERT_EQ(100, termHash);
    ASSERT_EQ("ut", term);
    ASSERT_FALSE(StatisticsTermFormatter::ParseDataLine(invalidBlock2, pos, term, termHash).IsOK());
}

} // namespace indexlibv2::index
