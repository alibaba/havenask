#include "sql/ops/join/JoinBaseParamR.h"

#include "unittest/unittest.h"

using namespace std;

namespace sql {

class JoinBaseParamRTest : public TESTBASE {
public:
    JoinBaseParamRTest();
    ~JoinBaseParamRTest();
};

JoinBaseParamRTest::JoinBaseParamRTest() {}

JoinBaseParamRTest::~JoinBaseParamRTest() {}

TEST_F(JoinBaseParamRTest, testConvertFields) {
    {
        JoinBaseParamR joinParamR;
        joinParamR._systemFieldNum = 1;
        ASSERT_FALSE(joinParamR.convertFields());
    }
    {
        JoinBaseParamR joinParamR;
        ASSERT_TRUE(joinParamR.convertFields());
    }
    {
        JoinBaseParamR joinParamR;
        joinParamR._leftInputFields = {"1", "2"};
        ASSERT_FALSE(joinParamR.convertFields());
        joinParamR._rightInputFields = {"3"};
        joinParamR._outputFieldsForKernel = {"1", "2", "30"};
        ASSERT_TRUE(joinParamR.convertFields());
        ASSERT_EQ("1", joinParamR._output2InputMap["1"].first);
        ASSERT_TRUE(joinParamR._output2InputMap["1"].second);
        ASSERT_EQ("2", joinParamR._output2InputMap["2"].first);
        ASSERT_TRUE(joinParamR._output2InputMap["2"].second);
        ASSERT_EQ("3", joinParamR._output2InputMap["30"].first);
        ASSERT_FALSE(joinParamR._output2InputMap["30"].second);
    }
}

TEST_F(JoinBaseParamRTest, testPatchHintInfo) {
    {
        JoinBaseParamR joinPR;
        joinPR.patchHintInfo();
        ASSERT_EQ(0, joinPR._defaultValue.size());
    }
    {
        JoinBaseParamR joinPR;
        joinPR._joinHintMap = {{"defaultValue", "a:1,b:2"}};
        joinPR.patchHintInfo();
        ASSERT_EQ(2, joinPR._defaultValue.size());
    }
}

} // namespace sql
