#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/test/QueryConstructor.h>
#include <ha3/common/NumberQuery.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class NumberQueryTest : public TESTBASE {
public:
    NumberQueryTest();
    ~NumberQueryTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, NumberQueryTest);


NumberQueryTest::NumberQueryTest() { 
}

NumberQueryTest::~NumberQueryTest() { 
}

void NumberQueryTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void NumberQueryTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(NumberQueryTest, testConstructor) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        RequiredFields requiredFields;
        NumberTerm term(10, "", requiredFields);
        string label("");
        NumberQuery numberQuery(term, label);
        ASSERT_EQ(MDL_TERM, numberQuery.getMatchDataLevel());
        ASSERT_EQ(label, numberQuery.getQueryLabel());
    }
    {
        RequiredFields requiredFields;
        NumberTerm term(10, "", requiredFields);
        string label("numberLabel");
        NumberQuery numberQuery(term, label);
        ASSERT_EQ(MDL_TERM, numberQuery.getMatchDataLevel());
        ASSERT_EQ(label, numberQuery.getQueryLabel());
    }
    {
        RequiredFields requiredFields;
        string label("");
        NumberQuery numberQuery(10, "", requiredFields, label);
        ASSERT_EQ(MDL_TERM, numberQuery.getMatchDataLevel());
        ASSERT_EQ(label, numberQuery.getQueryLabel());
    }
    {
        RequiredFields requiredFields;
        string label("numberLabel");
        NumberQuery numberQuery(10, "", requiredFields, label);
        ASSERT_EQ(MDL_TERM, numberQuery.getMatchDataLevel());
        ASSERT_EQ(label, numberQuery.getQueryLabel());
    }
    {
        RequiredFields requiredFields;
        string label("");
        NumberQuery numberQuery(10, false, 10, false, "", requiredFields, label);
        ASSERT_EQ(MDL_TERM, numberQuery.getMatchDataLevel());
        ASSERT_EQ(label, numberQuery.getQueryLabel());
    }
    {
        RequiredFields requiredFields;
        string label("numbelLabel");
        NumberQuery numberQuery(10, false, 10, false, "", requiredFields, label);
        ASSERT_EQ(MDL_TERM, numberQuery.getMatchDataLevel());
        ASSERT_EQ(label, numberQuery.getQueryLabel());
    }
}

TEST_F(NumberQueryTest, testSetQueryLabelWithDefaultLevel) {
    RequiredFields requiredFields;
    NumberTerm term(10, "", requiredFields);
    string label("");
    NumberQuery numberQuery(term, label);
    numberQuery.setQueryLabelWithDefaultLevel("numberLabel");
    ASSERT_EQ(MDL_TERM, numberQuery.getMatchDataLevel());
    ASSERT_EQ("numberLabel", numberQuery.getQueryLabel());
}

END_HA3_NAMESPACE(common);


