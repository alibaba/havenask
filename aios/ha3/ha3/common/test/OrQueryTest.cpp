#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/common/OrQuery.h"

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(common);

class OrQueryTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, OrQueryTest);

void OrQueryTest::setUp() {
}

void OrQueryTest::tearDown() {
}

TEST_F(OrQueryTest, testConstructor) {
    {
        string label("");
        OrQuery orQuery(label);
        ASSERT_EQ(MDL_NONE, orQuery.getMatchDataLevel());
        ASSERT_EQ(label, orQuery.getQueryLabel());
    }
    {
        string label("orLabel");
        OrQuery orQuery(label);
        ASSERT_EQ(MDL_SUB_QUERY, orQuery.getMatchDataLevel());
        ASSERT_EQ(label, orQuery.getQueryLabel());
    }
}

TEST_F(OrQueryTest, testSetQueryLabelWithDefaultLevel) {
    OrQuery orQuery("");
    orQuery.setQueryLabelWithDefaultLevel("orLabel");
    ASSERT_EQ(MDL_SUB_QUERY, orQuery.getMatchDataLevel());
    ASSERT_EQ("orLabel", orQuery.getQueryLabel());
}

END_HA3_NAMESPACE();

