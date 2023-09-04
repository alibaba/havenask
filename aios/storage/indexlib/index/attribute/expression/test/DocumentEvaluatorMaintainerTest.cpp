#include "indexlib/index/attribute/expression/DocumentEvaluatorMaintainer.h"

#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/expression/DocumentEvaluator.h"
#include "indexlib/index/attribute/expression/FunctionConfig.h"
#include "indexlib/index/attribute/expression/test/AttributeExpressionTestHelper.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class DocumentEvaluatorMaintainerTest : public TESTBASE
{
public:
    DocumentEvaluatorMaintainerTest() = default;
    ~DocumentEvaluatorMaintainerTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void DocumentEvaluatorMaintainerTest::setUp() {}

void DocumentEvaluatorMaintainerTest::tearDown() {}

TEST_F(DocumentEvaluatorMaintainerTest, TestSimple)
{
    AttributeExpressionTestHelper<ft_uint32, false> helper;
    ASSERT_TRUE(helper.Init());

    auto schema = std::shared_ptr<config::TabletSchema>(
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema2.json").release());
    ASSERT_TRUE(schema);
    ASSERT_TRUE(schema->TEST_GetImpl()->AddIndexConfig(helper._config).IsOK());

    DocumentEvaluatorMaintainer matainer;
    std::vector<std::string> functionNames(
        {"expr1", AttributeFunctionConfig::ALWAYS_TRUE_FUNCTION_NAME, "expr2", "expr3"});
    ASSERT_TRUE(matainer.Init(helper._segments, schema, functionNames).IsOK());
    auto evaluators = matainer.GetAllEvaluators();
    ASSERT_EQ(4, evaluators.size());
    {
        auto evaluator = evaluators[0];
        ASSERT_EQ(DocumentEvaluatorBase::EvaluateType::ET_UINT32, evaluator->GetEvaluateType());
        auto typedEvaluator = std::dynamic_pointer_cast<DocumentEvaluator<uint32_t>>(evaluator);
        ASSERT_TRUE(typedEvaluator);
        helper.AddOneDoc(100);
        auto [status, value] = typedEvaluator->Evaluate(0);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(200, value);
    }
    {
        auto evaluator = evaluators[1];
        ASSERT_EQ(DocumentEvaluatorBase::EvaluateType::ET_BOOL, evaluator->GetEvaluateType());
        auto typedEvaluator = std::dynamic_pointer_cast<DocumentEvaluator<bool>>(evaluator);
        ASSERT_TRUE(typedEvaluator);
        auto [status, value] = typedEvaluator->Evaluate(0);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(value);
    }
    {
        auto evaluator = evaluators[2];
        ASSERT_EQ(DocumentEvaluatorBase::EvaluateType::ET_BOOL, evaluator->GetEvaluateType());
        auto typedEvaluator = std::dynamic_pointer_cast<DocumentEvaluator<bool>>(evaluator);
        ASSERT_TRUE(typedEvaluator);
        auto [status, value] = typedEvaluator->Evaluate(0);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(value);
    }
    {
        auto evaluator = evaluators[3];
        ASSERT_EQ(DocumentEvaluatorBase::EvaluateType::ET_BOOL, evaluator->GetEvaluateType());
        auto typedEvaluator = std::dynamic_pointer_cast<DocumentEvaluator<bool>>(evaluator);
        ASSERT_TRUE(typedEvaluator);
        auto [status, value] = typedEvaluator->Evaluate(0);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(value);
    }
}

TEST_F(DocumentEvaluatorMaintainerTest, TestCurrentTimeFunction)
{
    AttributeExpressionTestHelper<ft_uint32, false> helper;
    ASSERT_TRUE(helper.Init());
    auto schema = std::shared_ptr<config::TabletSchema>(
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema3.json").release());
    ASSERT_TRUE(schema);
    ASSERT_TRUE(schema->TEST_GetImpl()->AddIndexConfig(helper._config).IsOK());
    helper.AddOneDoc(100);
    helper.AddOneDoc(100);
    int64_t ret1, ret2;
    {
        DocumentEvaluatorMaintainer matainer;
        std::vector<std::string> functionNames({"expr1", "expr2"});
        ASSERT_TRUE(matainer.Init(helper._segments, schema, functionNames).IsOK());
        auto evaluators = matainer.GetAllEvaluators();
        ASSERT_EQ(2, evaluators.size());
        auto evaluator = evaluators[0];
        ASSERT_EQ(DocumentEvaluatorBase::EvaluateType::ET_INT64, evaluator->GetEvaluateType());
        auto typedEvaluator = std::dynamic_pointer_cast<DocumentEvaluator<int64_t>>(evaluator);
        ASSERT_TRUE(typedEvaluator);
        auto [status1, value1] = typedEvaluator->Evaluate(0);
        ASSERT_TRUE(status1.IsOK());
        sleep(2);
        auto [status2, value2] = typedEvaluator->Evaluate(1);
        ASSERT_TRUE(status2.IsOK());
        ASSERT_EQ(value1, value2);

        auto evaluator2 = evaluators[1];
        ASSERT_EQ(DocumentEvaluatorBase::EvaluateType::ET_INT64, evaluator2->GetEvaluateType());
        auto typedEvaluator2 = std::dynamic_pointer_cast<DocumentEvaluator<int64_t>>(evaluator2);
        ASSERT_TRUE(typedEvaluator2);
        sleep(2);
        auto [status3, value3] = typedEvaluator2->Evaluate(0);
        ASSERT_TRUE(status3.IsOK());
        ASSERT_EQ(value1, value3);

        ret1 = value1;
    }
    sleep(2);
    {
        DocumentEvaluatorMaintainer matainer;
        std::vector<std::string> functionNames({"expr1"});
        ASSERT_TRUE(matainer.Init(helper._segments, schema, functionNames).IsOK());
        auto evaluators = matainer.GetAllEvaluators();
        ASSERT_EQ(1, evaluators.size());
        auto evaluator = evaluators[0];
        ASSERT_EQ(DocumentEvaluatorBase::EvaluateType::ET_INT64, evaluator->GetEvaluateType());
        auto typedEvaluator = std::dynamic_pointer_cast<DocumentEvaluator<int64_t>>(evaluator);
        ASSERT_TRUE(typedEvaluator);
        auto [status1, value1] = typedEvaluator->Evaluate(0);
        ASSERT_TRUE(status1.IsOK());
        sleep(2);
        auto [status2, value2] = typedEvaluator->Evaluate(1);
        ASSERT_TRUE(status2.IsOK());
        ASSERT_EQ(value1, value2);
        ret2 = value1;
    }
    ASSERT_NE(ret1, ret2);
}

} // namespace indexlibv2::index
