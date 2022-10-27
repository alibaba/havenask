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
#include <ha3/turing/variant/SeekIteratorVariant.h>
#include <ha3/turing/variant/LayerMetasVariant.h>
#include <ha3/search/LayerMetas.h>
#include <basic_ops/ops/test/OpTestBase.h>
#include <ha3/turing/ops/agg/PrepareExpressionResourceOp.h>
#include <suez/turing/expression/syntax/SyntaxParser.h>
#include <suez/turing/expression/syntax/SyntaxExprValidator.h>
using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);


class SeekIteratorPrepareOpTest : public OpTestBase {
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

    virtual void prepareInvertedTableData(std::string &tableName,
            std::string &fields, std::string &indexes, std::string &attributes,
            std::string &summarys, std::string &truncateProfileStr,
            std::string &docs, int64_t &ttl)
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
               "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b";
        ttl = std::numeric_limits<int64_t>::max();
    }


public:
    
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("mySeekIteratorPrepareOp", "SeekIteratorPrepareOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))                
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(const vector<int32_t> &docIds, bool empty = false) {
        HA3_LOG(INFO, "-------check output--------");
        auto pOutputTensor = GetOutput(0);
        ASSERT_TRUE(pOutputTensor != nullptr);
        auto seekIteratorVariant = pOutputTensor->scalar<Variant>()().get<SeekIteratorVariant>();
        ASSERT_TRUE(seekIteratorVariant != nullptr);
        auto seekIterator = seekIteratorVariant->getSeekIterator();
        if (empty) {
            ASSERT_TRUE(seekIterator == nullptr);
            return;
        }
        ASSERT_TRUE(seekIterator != nullptr);
        ASSERT_TRUE(seekIterator->_indexPartitionReaderWrapper != nullptr);
        ASSERT_TRUE(seekIterator->_matchDocAllocator != nullptr);
        ASSERT_TRUE(seekIterator->_queryExecutor != nullptr);
        ASSERT_TRUE(seekIterator->_matchDocAllocator != nullptr);
        ASSERT_TRUE(seekIterator->_singleLayerSearcher != nullptr);
        for (auto doc : docIds) {
            MatchDoc matchDoc = seekIterator->seek();
            ASSERT_EQ(doc, matchDoc.getDocId());
        }
        MatchDoc matchDoc = seekIterator->seek();
        ASSERT_EQ(-1, matchDoc.getDocId());
    }

private:
    RequestPtr prepareRequest(string termStr,
                              bool hasFilterClause = false,
                              bool hasPkFilterClause = false)
    {
        RequestPtr request(new Request(&_pool));
        Term term;
        term.setWord(termStr.c_str());
        term.setIndexName("index_2");
        TermQuery *termQuery = new TermQuery(term, "");
        QueryClause *queryClause = new QueryClause(termQuery);
        request->setQueryClause(queryClause);

        ConfigClause * configClause = new ConfigClause();
        request->setConfigClause(configClause);
        if (hasFilterClause) {
            SyntaxParser parser;
            SyntaxExpr *expr =  parser.parseSyntax("attr1=1");
            VirtualAttributes virtualAttributes;
            SyntaxExprValidator validator(_sessionResource->tableInfo->getAttributeInfos(),
                    virtualAttributes, false);
            validator.validate(expr);
            FilterClause *filterClause = new FilterClause(expr);
            request->setFilterClause(filterClause);
        }
        if (hasPkFilterClause) {
            PKFilterClause *pkFilterClause = new PKFilterClause();
            pkFilterClause->setOriginalString("2");
            request->setPKFilterClause(pkFilterClause);
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

    LayerMetasVariant prepareLayerMetasVariant(vector<pair<int32_t, int32_t> > &ranges ) {
        LayerMetasPtr layerMetas(new LayerMetas(&_pool));
        if (ranges.size() > 0) {
            LayerMeta layerMeta(&_pool);
            layerMeta.quotaMode = QM_PER_LAYER;
            for (size_t idx = 0; idx < ranges.size(); idx++) {
                layerMeta.push_back(DocIdRange(ranges[idx].first, ranges[idx].second));
            }
            layerMetas->push_back(layerMeta);
        }
        return LayerMetasVariant(layerMetas);
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, SeekIteratorPrepareOpTest);

TEST_F(SeekIteratorPrepareOpTest, testPrepareSeekIterator) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}};
    LayerMetasVariant layerMetasVariant = prepareLayerMetasVariant(ranges);
    auto request = prepareRequest("a");
    ExpressionResourceVariant expressionResourceVariant = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {layerMetasVariant});
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    TF_ASSERT_OK(RunOpKernel());
    vector<int32_t> docIds= {0, 1};
    checkOutput(docIds);
}

TEST_F(SeekIteratorPrepareOpTest, testPrepareSeekIteratorWithFilter) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}};
    LayerMetasVariant layerMetasVariant = prepareLayerMetasVariant(ranges);
    auto request = prepareRequest("a", true);
    ExpressionResourceVariant expressionResourceVariant = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {layerMetasVariant});
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    TF_ASSERT_OK(RunOpKernel());
    vector<int32_t> docIds= {1};
    checkOutput(docIds);
}

TEST_F(SeekIteratorPrepareOpTest, testPrepareSeekIteratorWithPkFilter) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}};
    LayerMetasVariant layerMetasVariant = prepareLayerMetasVariant(ranges);
    auto request = prepareRequest("a", false, true);
    ExpressionResourceVariant expressionResourceVariant = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {layerMetasVariant});
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    TF_ASSERT_OK(RunOpKernel());
    vector<int32_t> docIds= {1};
    checkOutput(docIds);
}

TEST_F(SeekIteratorPrepareOpTest, testPrepareSeekIteratorMultiRange) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 0}, {2,2}};
    LayerMetasVariant layerMetasVariant = prepareLayerMetasVariant(ranges);
    auto request = prepareRequest("a");
    ExpressionResourceVariant expressionResourceVariant = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {layerMetasVariant});
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    TF_ASSERT_OK(RunOpKernel());
    vector<int32_t> docIds= {0, 2};
    checkOutput(docIds);
}

TEST_F(SeekIteratorPrepareOpTest, testPrepareSeekIteratorEmpty) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {};
    LayerMetasVariant layerMetasVariant = prepareLayerMetasVariant(ranges);
    auto request = prepareRequest("a");
    ExpressionResourceVariant expressionResourceVariant = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {layerMetasVariant});
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    TF_ASSERT_OK(RunOpKernel());
    vector<int32_t> docIds= {};
    checkOutput(docIds, true);
}


TEST_F(SeekIteratorPrepareOpTest, testCreateQueryExecutorFailed) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}};
    LayerMetasVariant layerMetasVariant = prepareLayerMetasVariant(ranges);
    auto request = prepareRequest("bb");
    ExpressionResourceVariant expressionResourceVariant = prepareExpressionResourceVariant(request);
    AddInputFromArray<Variant>(TensorShape({}), {layerMetasVariant});
    AddInputFromArray<Variant>(TensorShape({}), {expressionResourceVariant});
    Status status = RunOpKernel();
    ASSERT_TRUE(status.ok());
    vector<int32_t> docIds= {};
    checkOutput(docIds, true);
}

END_HA3_NAMESPACE();

