#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/agg/AggNormal.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class AggNormalTest : public OpTestBase {
public:
    AggNormalTest() {}
    ~AggNormalTest() {}
public:
    void setUp() override {
    }
    void tearDown() override {
    }
};

TEST_F(AggNormalTest, testTranslateNonExistAggFunc) {
    AggFuncManagerPtr aggFuncManager(new AggFuncManager());
    aggFuncManager->init("", {});
    aggFuncManager->registerFunctionInfos();
    std::vector<AggFuncDesc> aggFuncDesc;
    {
        AggFuncDesc desc;
        desc.type = "NORMAL";
        desc.funcName = "AVG";
        desc.inputs = {"$a"};
        desc.outputs = {"$avgA"};
        aggFuncDesc.push_back(desc);
    }
    {
        AggFuncDesc desc;
        desc.type = "NORMAL";
        desc.funcName = "SUM_NOT_EXIST";
        desc.inputs = {"$a"};
        desc.outputs = {"$sumA"};
        aggFuncDesc.push_back(desc);
    }


    std::vector<AggFuncDesc> localAggFuncDesc = aggFuncDesc;
    std::vector<AggFuncDesc> globalAggFuncDesc = aggFuncDesc;
    ASSERT_FALSE(AggNormal::translateAggFuncDesc(
                    aggFuncManager, true, localAggFuncDesc));
    ASSERT_FALSE(AggNormal::translateAggFuncDesc(
                    aggFuncManager, false, globalAggFuncDesc));
}

TEST_F(AggNormalTest, testTranslateAggFunc) {
    AggFuncManagerPtr aggFuncManager(new AggFuncManager());
    aggFuncManager->init("", {});
    aggFuncManager->registerFunctionInfos();
    std::vector<AggFuncDesc> aggFuncDesc;
    {
        AggFuncDesc desc;
        desc.type = "NORMAL";
        desc.funcName = "AVG";
        desc.inputs = {"$a"};
        desc.outputs = {"$avgA"};
        aggFuncDesc.push_back(desc);
    }
    {
        AggFuncDesc desc;
        desc.type = "NORMAL";
        desc.funcName = "SUM";
        desc.inputs = {"$a"};
        desc.outputs = {"$sumA"};
        aggFuncDesc.push_back(desc);
    }


    std::vector<AggFuncDesc> localAggFuncDesc = aggFuncDesc;
    std::vector<AggFuncDesc> globalAggFuncDesc = aggFuncDesc;
    ASSERT_TRUE(AggNormal::translateAggFuncDesc(
                    aggFuncManager, true, localAggFuncDesc));
    ASSERT_TRUE(AggNormal::translateAggFuncDesc(
                    aggFuncManager, false, globalAggFuncDesc));
    ASSERT_EQ(2, localAggFuncDesc[0].outputs.size());
    ASSERT_EQ("__0_AVG_idx_0", localAggFuncDesc[0].outputs[0]);
    ASSERT_EQ("__0_AVG_idx_1", localAggFuncDesc[0].outputs[1]);
    ASSERT_EQ(2, globalAggFuncDesc[0].inputs.size());
    ASSERT_EQ("__0_AVG_idx_0", globalAggFuncDesc[0].inputs[0]);
    ASSERT_EQ("__0_AVG_idx_1", globalAggFuncDesc[0].inputs[1]);

    ASSERT_EQ(1, localAggFuncDesc[1].outputs.size());
    ASSERT_EQ("__1_SUM_idx_0", localAggFuncDesc[1].outputs[0]);
    ASSERT_EQ(1, globalAggFuncDesc[1].inputs.size());
    ASSERT_EQ("__1_SUM_idx_0", globalAggFuncDesc[1].inputs[0]);
}

END_HA3_NAMESPACE(sql);
