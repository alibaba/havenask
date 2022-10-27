#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/common/RankQuery.h"

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(common);

class RankQueryTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, RankQueryTest);

void RankQueryTest::setUp() {
}

void RankQueryTest::tearDown() {
}

TEST_F(RankQueryTest, testConstructor) {
    {
        string label("");
        RankQuery rankQuery(label);
        ASSERT_EQ(MDL_NONE, rankQuery.getMatchDataLevel());
        ASSERT_EQ(label, rankQuery.getQueryLabel());
    }
    {
        string label("rankLabel");
        RankQuery rankQuery(label);
        ASSERT_EQ(MDL_SUB_QUERY, rankQuery.getMatchDataLevel());
        ASSERT_EQ(label, rankQuery.getQueryLabel());
    }
}

TEST_F(RankQueryTest, testSetQueryLabelWithDefaultLevel) {
    RankQuery rankQuery("");
    rankQuery.setQueryLabelWithDefaultLevel("rankLabel");
    ASSERT_EQ(MDL_SUB_QUERY, rankQuery.getMatchDataLevel());
    ASSERT_EQ("rankLabel", rankQuery.getQueryLabel());
}

END_HA3_NAMESPACE();

