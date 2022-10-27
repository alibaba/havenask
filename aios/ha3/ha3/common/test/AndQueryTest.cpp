#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/common/AndQuery.h"

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(common);

class AndQueryTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AndQueryTest);

void AndQueryTest::setUp() {
}

void AndQueryTest::tearDown() {
}

TEST_F(AndQueryTest, testConstructor) {
    {
        string label("");
        AndQuery andQuery(label);
        ASSERT_EQ(MDL_NONE, andQuery.getMatchDataLevel());
        ASSERT_EQ(label, andQuery.getQueryLabel());
    }
    {
        string label("andLabel");
        AndQuery andQuery(label);
        ASSERT_EQ(MDL_SUB_QUERY, andQuery.getMatchDataLevel());
        ASSERT_EQ(label, andQuery.getQueryLabel());
    }
}

TEST_F(AndQueryTest, testSetQueryLabelWithDefaultLevel) {
    AndQuery andQuery("");
    andQuery.setQueryLabelWithDefaultLevel("andLabel");
    ASSERT_EQ(MDL_SUB_QUERY, andQuery.getMatchDataLevel());
    ASSERT_EQ("andLabel", andQuery.getQueryLabel());
}

END_HA3_NAMESPACE();

