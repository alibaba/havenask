#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/util/EnvParser.h>

using namespace std;
using namespace testing;
using namespace autil::legacy;

BEGIN_HA3_NAMESPACE(util);

class EnvParserTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, EnvParserTest);

void EnvParserTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void EnvParserTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(EnvParserTest, testParseParaWays) {
    {
        vector<string> paraWaysVec;
        ASSERT_TRUE(EnvParser::parseParaWays("2,4,8,16", paraWaysVec));
        ASSERT_EQ(4, paraWaysVec.size());
        ASSERT_EQ(string("2"), paraWaysVec[0]);
        ASSERT_EQ(string("4"), paraWaysVec[1]);
        ASSERT_EQ(string("8"), paraWaysVec[2]);
        ASSERT_EQ(string("16"), paraWaysVec[3]);
    }
    {
        vector<string> paraWaysVec;
        ASSERT_TRUE(EnvParser::parseParaWays("2", paraWaysVec));
        ASSERT_EQ(1, paraWaysVec.size());
        ASSERT_EQ(string("2"), paraWaysVec[0]);
    }
    {
        vector<string> paraWaysVec;
        ASSERT_FALSE(EnvParser::parseParaWays("", paraWaysVec));
        ASSERT_EQ(0, paraWaysVec.size());
    }
    {
        vector<string> paraWaysVec;
        ASSERT_FALSE(EnvParser::parseParaWays("f", paraWaysVec));
        ASSERT_EQ(0, paraWaysVec.size());
    }
    {
        vector<string> paraWaysVec;
        ASSERT_FALSE(EnvParser::parseParaWays("2,4,8,16,32", paraWaysVec));
        ASSERT_EQ(0, paraWaysVec.size());
    }

}

TEST_F(EnvParserTest, testIsWayValid) {
    ASSERT_TRUE(EnvParser::isWayValid("2"));
    ASSERT_TRUE(EnvParser::isWayValid("4"));
    ASSERT_TRUE(EnvParser::isWayValid("8"));
    ASSERT_TRUE(EnvParser::isWayValid("16"));
    ASSERT_FALSE(EnvParser::isWayValid("0"));
    ASSERT_FALSE(EnvParser::isWayValid("1"));
    ASSERT_FALSE(EnvParser::isWayValid("17"));
    ASSERT_FALSE(EnvParser::isWayValid("32"));
}

END_HA3_NAMESPACE(util);
