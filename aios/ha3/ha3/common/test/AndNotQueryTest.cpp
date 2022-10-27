#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/common/AndNotQuery.h"

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(common);

class AndNotQueryTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AndNotQueryTest);

void AndNotQueryTest::setUp() {
}

void AndNotQueryTest::tearDown() {
}

TEST_F(AndNotQueryTest, testConstructor) {
    {
        string label("");
        AndNotQuery andNotQuery(label);
        ASSERT_EQ(MDL_NONE, andNotQuery.getMatchDataLevel());
        ASSERT_EQ(label, andNotQuery.getQueryLabel());
    }
    {
        string label("andNotLabel");
        AndNotQuery andNotQuery(label);
        ASSERT_EQ(MDL_SUB_QUERY, andNotQuery.getMatchDataLevel());
        ASSERT_EQ(label, andNotQuery.getQueryLabel());
    }
}

TEST_F(AndNotQueryTest, testSetQueryLabelWithDefaultLevel) {
    AndNotQuery andNotQuery("");
    andNotQuery.setQueryLabelWithDefaultLevel("andNotLabel");
    ASSERT_EQ(MDL_SUB_QUERY, andNotQuery.getMatchDataLevel());
    ASSERT_EQ("andNotLabel", andNotQuery.getQueryLabel());
}

END_HA3_NAMESPACE();

