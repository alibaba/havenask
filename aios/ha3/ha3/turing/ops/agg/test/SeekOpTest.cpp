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
#include <ha3/turing/ops/agg/SeekIteratorPrepareOp.h>
#include <suez/turing/expression/syntax/SyntaxParser.h>
#include <suez/turing/expression/syntax/SyntaxExprValidator.h>
using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);


class SeekOpTest : public OpTestBase {
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

    void makeOp(int batchSize = 0) {
        TF_ASSERT_OK(
                NodeDefBuilder("mySeekOp", "SeekOp")
                .Input(FakeInput(DT_VARIANT))
                .Attr("batch_size", batchSize)
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(bool isEmpty, const vector<int32_t> &docIds, bool eof) {
        auto iteratorTensor = GetOutput(0);
        ASSERT_TRUE(iteratorTensor != nullptr);
        auto seekIteratorVariant = iteratorTensor->scalar<Variant>()().get<SeekIteratorVariant>();
        ASSERT_TRUE(seekIteratorVariant != nullptr);
        auto seekIterator = seekIteratorVariant->getSeekIterator();

        auto matchDocsTensor = GetOutput(1);
        ASSERT_TRUE(matchDocsTensor != nullptr);
        auto matchDocsVariant = matchDocsTensor->scalar<Variant>()().get<MatchDocsVariant>();
        ASSERT_TRUE(matchDocsVariant != nullptr);
        auto matchDocs = matchDocsVariant->getMatchDocs();

        auto eofTensor = GetOutput(2);
        ASSERT_TRUE(eofTensor != nullptr);
        bool eofFlag = eofTensor->scalar<bool>()();

        if (isEmpty) {
            ASSERT_TRUE(seekIterator == nullptr);
            ASSERT_EQ(0, matchDocs.size());
            ASSERT_TRUE(matchDocsVariant->getAllocator() == NULL);
            ASSERT_TRUE(eofFlag);
            return;
        }
        ASSERT_TRUE(seekIterator != nullptr);
        ASSERT_TRUE(seekIterator->_indexPartitionReaderWrapper != nullptr);
        ASSERT_TRUE(seekIterator->_matchDocAllocator != nullptr);
        ASSERT_TRUE(seekIterator->_queryExecutor != nullptr);
        ASSERT_TRUE(seekIterator->_matchDocAllocator != nullptr);
        ASSERT_TRUE(seekIterator->_singleLayerSearcher != nullptr);

        ASSERT_EQ(docIds.size(), matchDocs.size());

        for (size_t i = 0; i < docIds.size(); i++) {
            ASSERT_EQ(docIds[i], matchDocs[i].getDocId());
        }
        ASSERT_EQ(eof, eofFlag);
    }

private:
    RequestPtr prepareRequest(const string &termStr,
                              const string &filterStr = "",
                              const string &pkFilterStr= "")
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
        if (!filterStr.empty()) {
            SyntaxParser parser;
            SyntaxExpr *expr =  parser.parseSyntax(filterStr);
            VirtualAttributes virtualAttributes;
            SyntaxExprValidator validator(_sessionResource->tableInfo->getAttributeInfos(),
                    virtualAttributes, false);
            validator.validate(expr);
            FilterClause *filterClause = new FilterClause(expr);
            request->setFilterClause(filterClause);
        }
        if (!pkFilterStr.empty()) {
            PKFilterClause *pkFilterClause = new PKFilterClause();
            pkFilterClause->setOriginalString(pkFilterStr);
            request->setPKFilterClause(pkFilterClause);
        }

        return request;
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

HA3_LOG_SETUP(turing, SeekOpTest);

TEST_F(SeekOpTest, testSeekEmptyIterator) {
    makeOp();
    SeekIteratorVariant iteratorVariant;
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(true, {}, true );
}

TEST_F(SeekOpTest, testSeekSubRange) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 0}};
    auto request = prepareRequest("a");
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {0}, true);
}

TEST_F(SeekOpTest, testSeekSomeRange) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}, {5, 7}, {9, 9}};
    auto request = prepareRequest("a");
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {0, 1, 5 ,7 }, true);
}

TEST_F(SeekOpTest, testSeekAllRange) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("a");
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {0, 1, 2, 4, 5 ,7 ,8 }, true);
}
TEST_F(SeekOpTest, testSeekAllRange2) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("b");
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {3, 6, 9 }, true);
}

TEST_F(SeekOpTest, testSeekWithFilter) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}, {2, 4}, {5, 7}, {8, 9}};
    auto request = prepareRequest("a", "attr1=1");
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {1, 7}, true);
}

TEST_F(SeekOpTest, testSeekWithPkFilter) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("a", "", "2");
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {1}, true);
}
TEST_F(SeekOpTest, testSeekWithFilterAndPkFilter) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("a", "attr1=1", "3");
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {}, true);
}

TEST_F(SeekOpTest, testBatchSeek_Next) {
    makeOp(4);
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("a");
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {0, 1, 2, 4}, false);

    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {5, 7, 8}, true);

}

TEST_F(SeekOpTest, testBatchSeek2) {
    makeOp(3);
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("b");
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {3, 6, 9 }, false);
}

TEST_F(SeekOpTest, testBatchSeek3_EOF) {
    makeOp(4);
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("b");
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {3, 6, 9 }, true);
}

END_HA3_NAMESPACE();
