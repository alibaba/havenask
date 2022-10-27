#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/ExpressionResourceVariant.h>
#include <basic_ops/ops/test/OpTestBase.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);


class PrepareExpressionResourceOpTest : public OpTestBase {
public:
    void SetUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
        OpTestBase::SetUp();
    }
    void TearDown() override {
        OpTestBase::TearDown();
    }

    void prepareUserIndex() override {
        prepareInvertedTable();
    }

public:
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myPrepareExpressionResourceOp", "PrepareExpressionResourceOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput() {
        auto pOutputTensor = GetOutput(0);
        ASSERT_TRUE(pOutputTensor != nullptr);

        auto expressionResourceVariant = pOutputTensor->scalar<Variant>()().get<ExpressionResourceVariant>();
        ASSERT_TRUE(expressionResourceVariant != nullptr);

        ExpressionResourcePtr resource = expressionResourceVariant->getExpressionResource();
        ASSERT_TRUE(resource != nullptr);
        ASSERT_TRUE(resource->_matchDocAllocator != nullptr);
        ASSERT_TRUE(resource->_request != nullptr);
        ASSERT_TRUE(resource->_functionProvider != nullptr);
        ASSERT_TRUE(resource->_attributeExpressionCreator != nullptr);        
    }

private:
    Ha3RequestVariant prepareInput(bool useSub) {
        RequestPtr request(new Request(&_pool));
        VirtualAttributeClause *virtualAttributeClause = new VirtualAttributeClause();;
        SyntaxExpr *syntaxExpr = new RankSyntaxExpr();
        virtualAttributeClause->addVirtualAttribute("expr-name", syntaxExpr);
        request->setVirtualAttributeClause(virtualAttributeClause);
        ConfigClause *configClause = new ConfigClause();
        if (useSub) {
            configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_NO);
        }
        request->setConfigClause(configClause);
        return Ha3RequestVariant(request, &_pool);
    }
};


TEST_F(PrepareExpressionResourceOpTest, testDefaultRequest) {
    makeOp();
    Ha3RequestVariant variant = prepareInput(false);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput();
}

TEST_F(PrepareExpressionResourceOpTest, testDefaultRequest_UseSub) {
    makeOp();
    Ha3RequestVariant variant = prepareInput(true);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput();
}

END_HA3_NAMESPACE();

