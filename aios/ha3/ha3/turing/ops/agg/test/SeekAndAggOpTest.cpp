#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/common/Result.h>
#include <ha3/common/TermQuery.h>
#include <ha3/turing/variant/ExpressionResourceVariant.h>
#include <ha3/turing/variant/AggregatorVariant.h>
#include <ha3/turing/variant/SeekIteratorVariant.h>
#include <basic_ops/ops/test/OpTestBase.h>
#include <ha3/turing/ops/agg/PrepareExpressionResourceOp.h>
#include <ha3/turing/ops/agg/SeekIteratorPrepareOp.h>
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


class SeekAndAggOpTest : public OpTestBase {
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

    void makeOp(int batchSize = -1) {
        TF_ASSERT_OK(
                NodeDefBuilder("mySeekAndAggOp", "SeekAndAggOp")
                .Attr("batch_size", batchSize)
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(const vector<int> &groupCounts, uint64_t seekCount, uint64_t seekTermDocCount) {
        auto aggTensor = GetOutput(0);
        ASSERT_TRUE(aggTensor != nullptr);
        auto aggVariant = aggTensor->scalar<Variant>()().get<AggregatorVariant>();
        ASSERT_TRUE(aggVariant != nullptr);
        auto agg = aggVariant->getAggregator();
        ASSERT_TRUE(agg != nullptr);
        agg->endLayer(1);

        auto aggResult = (*agg->collectAggregateResult())[0];
        string attrStr = aggResult->getGroupExprStr();
        auto aggAllocator = aggResult->getMatchDocAllocator();
        auto aggDocs = aggResult->getAggResultVector();
        Reference<int64_t> *ref = aggAllocator->findReference<int64_t>("count");
        ASSERT_EQ(groupCounts.size(), aggDocs.size());
        for (size_t idx = 0; idx < groupCounts.size(); idx++) {
            int32_t count = ref->get(aggDocs[idx]);
            ASSERT_EQ(groupCounts[idx], count);
        }
        ASSERT_EQ(seekCount, aggVariant->getSeekedCount());
        ASSERT_EQ(seekTermDocCount, aggVariant->getSeekTermDocCount());
    }

private:
    RequestPtr prepareRequest(const string &termStr, const string &aggStr, const string &aggType, uint32_t rankSize = 0) {
        RequestPtr request(new Request(&_pool));
        Term term;
        term.setWord(termStr.c_str());
        term.setIndexName("index_2");
        TermQuery *termQuery = new TermQuery(term, "");
        QueryClause *queryClause = new QueryClause(termQuery);
        request->setQueryClause(queryClause);
        ConfigClause * configClause = new ConfigClause();
        configClause->setRankSize(rankSize);
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

    SeekIteratorVariant prepareIteratorVariant(vector<pair<int32_t, int32_t> > &ranges,
            RequestPtr request)
    {
        LayerMetasPtr layerMetas = createLayerMetas(ranges);
        auto queryResource = getQueryResource().get();
        string mainTableName = _sessionResource->bizInfo._itemTableName;
        ExpressionResourcePtr resource = PrepareExpressionResourceOp::createExpressionResource(
                request, _sessionResource->tableInfo, mainTableName,
                _sessionResource->functionInterfaceCreator,
                queryResource->getIndexSnapshot(), queryResource->getTracer(),
                _sessionResource->cavaPluginManager, &_pool, nullptr);
        SeekIteratorPtr seekIterator;
        SeekIteratorPrepareOp::createSeekIterator(mainTableName, resource, layerMetas,
                queryResource, seekIterator);
        SeekIteratorVariant variant(seekIterator, layerMetas, resource);
        return variant;
    }

    LayerMetasPtr createLayerMetas(vector<pair<int32_t, int32_t> > &ranges ) {
        LayerMetasPtr layerMetas(new LayerMetas(&_pool));
        if (ranges.size() > 0) {
            LayerMeta layerMeta(&_pool);
            layerMeta.quotaMode = QM_PER_LAYER;
            for (size_t idx = 0; idx < ranges.size(); idx++) {
                layerMeta.push_back(DocIdRange(ranges[idx].first, ranges[idx].second));
            }
            layerMetas->push_back(layerMeta);
        }
        return layerMetas;
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, SeekAndAggOpTest);

TEST_F(SeekAndAggOpTest, testNoRankSize) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("a", "attr1", "count");
    auto aggVariant = prepareAggVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    auto iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 2, 4}, 7, 8);
}

TEST_F(SeekAndAggOpTest, testRankSize) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("a", "attr1", "count", 5);
    auto aggVariant = prepareAggVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    auto iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 1, 3}, 5, 5);
}

TEST_F(SeekAndAggOpTest, testBatchSize) {
    makeOp(2);
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("a", "attr1", "count", 5);
    auto aggVariant = prepareAggVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    auto iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 1, 3}, 5, 5);
}

TEST_F(SeekAndAggOpTest, testBatchSizeNoRankSize) {
    makeOp(2);
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("a", "attr1", "count");
    auto aggVariant = prepareAggVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    auto iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 2, 4}, 7, 8);
}

TEST_F(SeekAndAggOpTest, testBigBatchSizeRankSize) {
    makeOp(3);
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("a", "attr1", "count", 6);
    auto aggVariant = prepareAggVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    auto iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 2, 3}, 6, 6);
}

TEST_F(SeekAndAggOpTest, testNoIterator) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("a", "attr1", "count", 5);
    auto aggVariant = prepareAggVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    auto iteratorVariant = SeekIteratorVariant();
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({}, 0, 0);
}

END_HA3_NAMESPACE();
