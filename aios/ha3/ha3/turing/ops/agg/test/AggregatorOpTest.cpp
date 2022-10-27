#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/common/TermQuery.h>
#include <ha3/turing/variant/ExpressionResourceVariant.h>
#include <ha3/turing/variant/AggregatorVariant.h>
#include <basic_ops/ops/test/OpTestBase.h>
#include <ha3/turing/ops/agg/PrepareExpressionResourceOp.h>
#include <ha3/turing/ops/agg/AggPrepareOp.h>
#include <suez/turing/expression/syntax/SyntaxParser.h>
#include <suez/turing/expression/syntax/SyntaxExprValidator.h>
using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);


class AggregatorOpTest : public OpTestBase {
public:
    void SetUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
        OpTestBase::SetUp();
    }

    void TearDown() override {
        OpTestBase::TearDown();
        gtl::STLDeleteElements(&tensors_); // clear output for using pool
    }

    void prepareUserIndex() override {
        prepareInvertedTable();
    }

    void prepareInvertedTableData(std::string &tableName,
                                  std::string &fields,
                                  std::string &indexes,
                                  std::string &attributes,
                                  std::string &summarys,
                                  std::string &truncateProfileStr,
                                  std::string &docs,
                                  int64_t &ttl) override
    {
        tableName = "invertedTable";
        fields = "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:string";
        indexes = "id:primarykey64:id;index_2:string:index2;name:string:name";
        attributes = "attr1;attr2";
        summarys = "name";
        truncateProfileStr = "";
        docs = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a;"
               "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a;"
               "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a;"
               "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b;"
               "cmd=add,attr1=2,attr2=1 11,id=5,name=bb,index2=a;"
               "cmd=add,attr1=2,attr2=2 22,id=6,name=cc,index2=a;"
               "cmd=add,attr1=1,attr2=3 33,id=7,name=dd,index2=b;"
               "cmd=add,attr1=1,attr2=1 11,id=8,name=bb,index2=a;"
               "cmd=add,attr1=2,attr2=2 22,id=9,name=cc,index2=a;"
               "cmd=add,attr1=3,attr2=3 33,id=10,name=dd,index2=b";
        ttl = std::numeric_limits<int64_t>::max();
    }

public:

    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myAggregatorOp", "AggregatorOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(int expectCount, const vector<int32_t> &docIds) {
        auto aggTensor = GetOutput(0);
        ASSERT_TRUE(aggTensor != nullptr);
        auto aggVariant = aggTensor->scalar<Variant>()().get<AggregatorVariant>();
        ASSERT_TRUE(aggVariant != nullptr);
        auto agg = aggVariant->getAggregator();
        ASSERT_TRUE(agg != nullptr);

        auto docsTensor = GetOutput(1);
        ASSERT_TRUE(docsTensor != nullptr);
        auto docsVariant = docsTensor->scalar<Variant>()().get<MatchDocsVariant>();
        ASSERT_TRUE(docsVariant != nullptr);
        auto allocator = docsVariant->getAllocator();
        ASSERT_TRUE(allocator != nullptr);

        auto matchDocs = docsVariant->getMatchDocs();
        ASSERT_EQ(docIds.size(), matchDocs.size());
        for (size_t i = 0; i < docIds.size(); i++) {
            ASSERT_EQ(docIds[i], matchDocs[i].getDocId());
        }
        agg->endLayer(1);
        auto aggResultVector = agg->collectAggregateResult();
        ASSERT_TRUE(aggResultVector != nullptr);
        ASSERT_EQ(1, aggResultVector->size());
        auto aggResult = (*aggResultVector)[0];

        string attrStr = aggResult->getGroupExprStr();
        auto aggAllocator = aggResult->getMatchDocAllocator();
        auto aggDocs = aggResult->getAggResultVector();
        Reference<int64_t> *ref = aggAllocator->findReference<int64_t>("count");
        ASSERT_EQ(1, aggDocs.size());
        int32_t count = ref->get(aggDocs[0]);
        ASSERT_EQ(expectCount, count);
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

    AggregatorVariant prepareAggVariant(RequestPtr request) {
        auto queryResource = getQueryResource().get();
        string mainTableName = _sessionResource->bizInfo._itemTableName;
        ExpressionResourcePtr resource = PrepareExpressionResourceOp::createExpressionResource(
                request, _sessionResource->tableInfo, mainTableName,
                _sessionResource->functionInterfaceCreator,
                queryResource->getIndexSnapshot(), queryResource->getTracer(),
                _sessionResource->cavaPluginManager, &_pool, nullptr);
        AggregatorPtr agg = AggPrepareOp::createAggregator(resource, queryResource);
        AggregatorVariant variant(agg, resource);
        return variant;
    }
    MatchDocsVariant prepareMatchDocsVariant(const vector<int32_t> &docIds,
            const MatchDocAllocatorPtr &allocator) {
        MatchDocsVariant variant(allocator, &_pool);
        for (auto docId : docIds) {
            variant.allocate(docId);
        }
        return variant;
    }
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, AggregatorOpTest);

TEST_F(AggregatorOpTest, testEmptyAggregator) {
    makeOp();
    AggregatorVariant aggVariant;
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    MatchDocsVariant docsVariant;
    AddInputFromArray<Variant>(TensorShape({}), {docsVariant});
    Status status = RunOpKernel();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ("aggregator is empty.", status.error_message());
}

TEST_F(AggregatorOpTest, testCountAggregator) {
    makeOp();
    auto request = prepareRequest("a", "attr1", "count");
    AggregatorVariant aggVariant = prepareAggVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    MatchDocsVariant docsVariant = prepareMatchDocsVariant({1, 7},
            aggVariant.getExpressionResource()->_matchDocAllocator);
    AddInputFromArray<Variant>(TensorShape({}), {docsVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(2, {1, 7});
}


END_HA3_NAMESPACE();
