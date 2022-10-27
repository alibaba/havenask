#include "indexlib/misc/test/regular_expression_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(misc);
IE_LOG_SETUP(misc, RegularExpressionTest);

RegularExpressionTest::RegularExpressionTest()
{
}

RegularExpressionTest::~RegularExpressionTest()
{
}

void RegularExpressionTest::SetUp()
{
}

void RegularExpressionTest::TearDown()
{
}

void RegularExpressionTest::TestMatch()
{
    {
        RegularExpression regexp;
        INDEXLIB_TEST_TRUE(regexp.Init(".*/dictionary$"));
        INDEXLIB_TEST_TRUE(regexp.Match("segment_1/index/pid/dictionary"));
        INDEXLIB_TEST_TRUE(!regexp.Match("segment_1/index/pid/posting"));
    }
    {
        RegularExpression regexp;
        INDEXLIB_TEST_TRUE(regexp.Init("^segment_[1-9][0-9]*"));
        INDEXLIB_TEST_TRUE(regexp.Match("segment_1/index/pid/posting"));
        INDEXLIB_TEST_TRUE(!regexp.Match("segment_0/index/pid/posting"));
        INDEXLIB_TEST_TRUE(regexp.Match("segment_20/index/pid/posting"));
    }
    {
        RegularExpression regexp;
        INDEXLIB_TEST_TRUE(regexp.Init("ops_multi_attr"));
        INDEXLIB_TEST_TRUE(regexp.Match("segment_1/attribute/ops_multi_attr/data"));
        INDEXLIB_TEST_TRUE(!regexp.Match("segment_1/attribute/ops_single_attr/data"));
    }
    {
        RegularExpression regexp;
        INDEXLIB_TEST_TRUE(regexp.Init("^segment_[0-9]+/index/"));
        INDEXLIB_TEST_TRUE(regexp.Match("segment_1/index/title/posting"));
        INDEXLIB_TEST_TRUE(!regexp.Match("ppp/segment_1/index/title/posting"));
    }
}

void RegularExpressionTest::TestNotInit()
{
    RegularExpression regexp;
    INDEXLIB_TEST_TRUE(!regexp.Match("anything"));
    INDEXLIB_TEST_EQUAL("Not Call Init", regexp.GetLatestErrMessage());
}

void RegularExpressionTest::TestGetLatestErrMessage()
{
    RegularExpression regexp;
    INDEXLIB_TEST_TRUE(regexp.Init(".*/dictionary"));
    INDEXLIB_TEST_TRUE(!regexp.Match("not_match_string"));
    INDEXLIB_TEST_EQUAL("No match", regexp.GetLatestErrMessage());
}

IE_NAMESPACE_END(misc);

