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
#include <ha3/turing/variant/AggregateResultsVariant.h>
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


class AggMergeOpTest : public OpTestBase {
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

    void makeOp(int count = 1) {
        TF_ASSERT_OK(
                NodeDefBuilder("myAggMergeOp", "AggMergeOp")
                .Input(FakeInput(count, DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(const vector<int> &groupCounts, uint32_t aggCount, uint32_t toAggCount,
                     uint64_t seekCount, uint64_t seekTermDocCount)
    {
        auto resultTensor = GetOutput(0);
        ASSERT_TRUE(resultTensor != nullptr);
        auto aggResultsVariant = resultTensor->scalar<Variant>()().get<AggregateResultsVariant>();
        ASSERT_TRUE(aggResultsVariant != nullptr);
        ASSERT_EQ(aggCount, aggResultsVariant->getAggCount());
        ASSERT_EQ(toAggCount, aggResultsVariant->getToAggCount());
        ASSERT_EQ(seekCount, aggResultsVariant->getSeekedCount());
        ASSERT_EQ(seekTermDocCount, aggResultsVariant->getSeekTermDocCount());

        auto aggResults = aggResultsVariant->getAggResults();
        ASSERT_TRUE(aggResults != nullptr);
        ASSERT_EQ(1, aggResults->size());

        auto aggResult = (*aggResults)[0];
        string attrStr = aggResult->getGroupExprStr();
        auto aggAllocator = aggResult->getMatchDocAllocator();
        auto aggDocs = aggResult->getAggResultVector();
        Reference<int64_t> *ref = aggAllocator->findReference<int64_t>("count");
        ASSERT_EQ(groupCounts.size(), aggDocs.size());
        vector<int> counts;
        for (size_t idx = 0; idx < groupCounts.size(); idx++) {
            int32_t count = ref->get(aggDocs[idx]);
            counts.push_back(count);
        }
        sort(counts.begin(),counts.end());
        ASSERT_EQ(groupCounts,counts);
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

    std::vector<matchdoc::MatchDoc> prepareMatchDocs(const vector<int32_t> &docIds,
            MatchDocAllocatorPtr &allocator) {
        std::vector<matchdoc::MatchDoc> docs;
        for (auto docId : docIds) {
            auto doc = allocator->allocate(docId);
            docs.push_back(doc);
        }
        return docs;
    }

    AggregatorVariant prepareAggVariant(RequestPtr request, const vector<int32_t>& docIds,
            uint64_t seekCount, uint64_t seekTermDocCount)
    {
        auto queryResource = getQueryResource().get();
        string mainTableName = _sessionResource->bizInfo._itemTableName;
        ExpressionResourcePtr resource = PrepareExpressionResourceOp::createExpressionResource(
                request, _sessionResource->tableInfo, mainTableName,
                _sessionResource->functionInterfaceCreator,
                queryResource->getIndexSnapshot(), queryResource->getTracer(),
                _sessionResource->cavaPluginManager, &_pool, nullptr);
        AggregatorPtr agg = AggPrepareOp::createAggregator(resource, queryResource);
        auto docs = prepareMatchDocs(docIds, resource->_matchDocAllocator);
        agg->batchAggregate(docs);
        AggregatorVariant variant(agg, resource);
        variant.setSeekedCount(seekCount);
        variant.setSeekTermDocCount(seekTermDocCount);
        return variant;
    }
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, AggMergeOpTest);

TEST_F(AggMergeOpTest, testOneAggregator) {
    makeOp();
    auto request = prepareRequest("a", "attr1", "count");
    AggregatorVariant aggVariant = prepareAggVariant(request, {1, 7}, 3, 4);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({2}, 2, 2, 3, 4);
}

TEST_F(AggMergeOpTest, testTwoAggregator) {
    makeOp(2);
    auto request = prepareRequest("a", "attr1", "count");
    AggregatorVariant aggVariant1 = prepareAggVariant(request, {1, 3}, 3, 4);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant1});
    AggregatorVariant aggVariant2 = prepareAggVariant(request, {5, 7}, 5 ,6);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant2});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 1, 2}, 4, 4, 8, 10);
}


END_HA3_NAMESPACE();
