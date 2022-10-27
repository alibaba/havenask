#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/common/TermQuery.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/turing/variant/ExpressionResourceVariant.h>
#include <ha3/turing/variant/AggregatorVariant.h>
#include <ha3/turing/ops/agg/PrepareExpressionResourceOp.h>
#include <ha3/common/Request.h>
#include <ha3/common/AggregateDescription.h>
#include <basic_ops/ops/test/OpTestBase.h>
#include <suez/turing/expression/syntax/SyntaxParser.h>
#include <suez/turing/expression/syntax/SyntaxExprValidator.h>
#include <ha3/turing/ops/agg/AggPrepareOp.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);


class AggPrepareOpTest : public OpTestBase {
public:
    void SetUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
        OpTestBase::SetUp();
        string configPath = GET_TEST_DATA_PATH() + "ops_test/agg_op_test/prepare_agg_op";
        _sessionResource->resourceReader.reset(new resource_reader::ResourceReader(configPath));
    }

    void TearDown() override {
        OpTestBase::TearDown();
        gtl::STLDeleteElements(&tensors_); // clear output for using pool
    }
public:
    void prepareUserIndex() override {
        prepareInvertedTable();
    }

    void makeOp(string relativePath="", string jsonPath = "") {
        TF_ASSERT_OK(
                NodeDefBuilder("myAggPrepareOp", "AggPrepareOp")
                .Attr("relative_path", relativePath)
                .Attr("json_path", jsonPath)
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(bool isEmpty = false) {
        auto pOutputTensor = GetOutput(0);
        ASSERT_TRUE(pOutputTensor != nullptr);
        auto aggregatorVariant = pOutputTensor->scalar<Variant>()().get<AggregatorVariant>();
        ASSERT_TRUE(aggregatorVariant != nullptr);
        auto aggregator = aggregatorVariant->getAggregator();
        ASSERT_EQ(isEmpty, aggregator == nullptr);
    }

private:
    RequestPtr prepareRequest(const string &termStr, const string &aggStr, const string &aggType) {
        RequestPtr request(new Request(&_pool));
        Term term;
        term.setWord(termStr.c_str());
        term.setIndexName("index_2");
        TermQuery *termQuery = new TermQuery(term, "");
        QueryClause *queryClause = new QueryClause(termQuery);
        request->setQueryClause(queryClause);
        ConfigClause * configClause = new ConfigClause();
        request->setConfigClause(configClause);
        if (!aggStr.empty()) {
            AggregateClause *aggClause = new AggregateClause();
            AggregateDescription *aggDescription = new AggregateDescription();
            SyntaxParser parser;
            SyntaxExpr *expr =  parser.parseSyntax(aggStr);
            VirtualAttributes virtualAttributes;
            SyntaxExprValidator validator(_sessionResource->tableInfo->getAttributeInfos(),
                    virtualAttributes, false);
            validator.validate(expr);
            aggDescription->setGroupKeyExpr(expr);
            AggFunDescription *func = new AggFunDescription(aggType, NULL);
            aggDescription->appendAggFunDescription(func);
            aggClause->addAggDescription(aggDescription);
            request->setAggregateClause(aggClause);
        }
        return request;
    }

    ExpressionResourceVariant prepareExpressionResourceVariant(RequestPtr request) {
        auto queryResource = getQueryResource();
        ExpressionResourcePtr resource = PrepareExpressionResourceOp::createExpressionResource(
                request, _sessionResource->tableInfo, _sessionResource->bizInfo._itemTableName,
                _sessionResource->functionInterfaceCreator,
                queryResource->getIndexSnapshot(), queryResource->getTracer(),
                _sessionResource->cavaPluginManager, &_pool, nullptr);
        return ExpressionResourceVariant(resource);
    }

};

TEST_F(AggPrepareOpTest, testEmptyAgg) {
    makeOp();
    auto request = prepareRequest("a", "", "");
    auto expressionResourceVariant  = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    Status status = RunOpKernel();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ("prepare aggregator failed.", status.error_message());
}

TEST_F(AggPrepareOpTest, testErrorAggFunc) {
    makeOp();
    auto request = prepareRequest("a", "attr1", "count1");
    auto expressionResourceVariant  = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    Status status = RunOpKernel();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ("prepare aggregator failed.", status.error_message());
}

TEST_F(AggPrepareOpTest, testErrorAggDesc) {
    makeOp();
    auto request = prepareRequest("a", "attr111", "count");
    auto expressionResourceVariant  = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    Status status = RunOpKernel();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ("prepare aggregator failed.", status.error_message());
}

TEST_F(AggPrepareOpTest, testErrorAggFuncType) {
    makeOp();
    auto request = prepareRequest("a", "attr1", "max");
    auto expressionResourceVariant  = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    Status status = RunOpKernel();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ("prepare aggregator failed.", status.error_message());
}

TEST_F(AggPrepareOpTest, testRequest) {
    makeOp();
    auto request = prepareRequest("a", "attr1", "count");
    auto expressionResourceVariant  = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput();
}

TEST_F(AggPrepareOpTest, testReadAggSampleConfig) {
    makeOp("agg_config_1.json", "aggregate_sampler_config");
    AggPrepareOp *aggOp = dynamic_cast<AggPrepareOp*>(kernel_.get());
    ASSERT_TRUE(aggOp != NULL);
    ASSERT_FALSE(aggOp->_aggSamplerConfig.getAggBatchMode());
    auto request = prepareRequest("a", "attr1", "count");
    auto expressionResourceVariant  = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput();
}

END_HA3_NAMESPACE();
