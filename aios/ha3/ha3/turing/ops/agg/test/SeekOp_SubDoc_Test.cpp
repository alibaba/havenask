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
#include <matchdoc/SubDocAccessor.h>
using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);


class SeekOpSubDocTest : public OpTestBase {
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
    void prepareInvertedTable() override {
        string tableName = "test_index";
        std::string testPath = _testPath + tableName;
        auto indexPartition = makeIndexPartition(testPath, tableName);
        std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _bizInfo->_itemTableName = schemaName;
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
    }

    IE_NAMESPACE(partition)::IndexPartitionPtr makeIndexPartition(const std::string &rootPath,
            const std::string &tableName)
    {
        int64_t ttl = std::numeric_limits<int64_t>::max();
        auto mainSchema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
                tableName,
                "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:string", //fields
                "id:primarykey64:id;index_2:string:index2;name:string:name", //fields
                "attr1;attr2", //attributes
                "name", // summary
                "");// truncateProfile

        auto subSchema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
                tableName,
                "sub_id:int64;sub_attr1:int32;sub_index2:string", // fields
                "sub_id:primarykey64:sub_id;sub_index_2:string:sub_index2", //indexs
                "sub_attr1", //attributes
                "", // summarys
                ""); // truncateProfile
        string docsStr = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a,sub_id=1^2^3,sub_attr1=1^2^2,sub_index2=aa^ab^ac;"
                         "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a,sub_id=4,sub_attr1=1,sub_index2=aa;"
                         "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a,sub_id=5,sub_attr1=1,sub_index2=ab;"
                         "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b,sub_id=6,sub_attr1=2,sub_index2=aa;"
                         "cmd=add,attr1=2,attr2=1 11,id=5,name=bb,index2=a,sub_id=7^8,sub_attr1=2^3,sub_index2=aa^ab;"
                         "cmd=add,attr1=2,attr2=2 22,id=6,name=cc,index2=a,sub_id=9,sub_attr1=2,sub_index2=aa;"
                         "cmd=add,attr1=1,attr2=3 33,id=7,name=dd,index2=b,sub_id=10,sub_attr1=1,sub_index2=ab;"
                         "cmd=add,attr1=1,attr2=1 11,id=8,name=bb,index2=a,sub_id=11,sub_attr1=2,sub_index2=aa;"
                         "cmd=add,attr1=2,attr2=2 22,id=9,name=cc,index2=a,sub_id=12,sub_attr1=2,sub_index2=abc;"
                         "cmd=add,attr1=3,attr2=3 33,id=10,name=dd,index2=b,sub_id=13^14,sub_attr1=2^1,sub_index2=abc^ab";

        IE_NAMESPACE(config)::IndexPartitionOptions options;
        options.GetOnlineConfig().ttl = ttl;
        auto schema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchemaWithSub(mainSchema, subSchema);

        IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
            IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                    schema, rootPath, docsStr, options, "", true);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }

public:

    void makeOp(int batchSize = 0) {
        TF_ASSERT_OK(
                NodeDefBuilder("mySeekOpSubDoc", "SeekOp")
                .Input(FakeInput(DT_VARIANT))
                .Attr("batch_size", batchSize)
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(bool isEmpty, const vector<int32_t> &docIds, bool eof,
                     const vector<vector<int32_t> > subDocs = {}) {
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

        auto allocator = matchDocsVariant->getAllocator();

        if (isEmpty) {
            ASSERT_TRUE(seekIterator == nullptr);
            ASSERT_EQ(0, matchDocs.size());
            ASSERT_TRUE(allocator == NULL);
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
        vector<int32_t> subDocIds;

        for (size_t i = 0; i < docIds.size(); i++) {
            if (!subDocs.empty()) {
                subDocIds.clear();
                auto accessor = allocator->getSubDocAccessor();
                accessor->getSubDocIds(matchDocs[i], subDocIds);
            }
        }

        for (size_t i = 0; i < docIds.size(); i++) {
            ASSERT_EQ(docIds[i], matchDocs[i].getDocId());
            if (!subDocs.empty()) {
                subDocIds.clear();
                auto accessor = allocator->getSubDocAccessor();
                accessor->getSubDocIds(matchDocs[i], subDocIds);
                ASSERT_EQ(subDocs[i], subDocIds);
            }
        }
        ASSERT_EQ(eof, eofFlag);
    }

private:
    RequestPtr prepareRequest(const string &indexStr,
                              const string &termStr,
                              const string &filterStr = "",
                              const string &pkFilterStr = "",
                              bool hasSubDoc = false)
    {
        RequestPtr request(new Request(&_pool));
        Term term;
        term.setWord(termStr.c_str());
        term.setIndexName(indexStr.c_str());
        TermQuery *termQuery = new TermQuery(term, "");
        QueryClause *queryClause = new QueryClause(termQuery);
        request->setQueryClause(queryClause);
        ConfigClause * configClause = new ConfigClause();
        if (hasSubDoc) {
            configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_FLAT);
        }
        request->setConfigClause(configClause);
        if (!filterStr.empty()) {
            SyntaxParser parser;
            SyntaxExpr *expr =  parser.parseSyntax(filterStr);
            VirtualAttributes virtualAttributes;
            SyntaxExprValidator validator(_sessionResource->tableInfo->getAttributeInfos(),
                    virtualAttributes, hasSubDoc);
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

HA3_LOG_SETUP(turing, SeekOpSubDocTest);

TEST_F(SeekOpSubDocTest, testGetAllSubDocs) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 5}};
    auto request = prepareRequest("index_2", "a", "", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {0, 1, 2, 4, 5}, true, {{0, 1, 2}, {3}, {4}, {6, 7}, {8}});
}

TEST_F(SeekOpSubDocTest, testGetAllSubDocsWithFilter) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 5}};
    auto request = prepareRequest("index_2", "a", "attr2=1", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {1, 4}, true, {{3}, {6, 7}});
}

TEST_F(SeekOpSubDocTest, testSeekSubDocs) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("sub_index_2", "abc", "", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {8, 9}, true, {{11}, {12}});
}

TEST_F(SeekOpSubDocTest, testSeekSubDocWithFilterMainDoc) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("sub_index_2", "aa", "attr1=1", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {1, 7}, true, {{3}, {10}});
}
TEST_F(SeekOpSubDocTest, testSeekSubDocWithFilterSubDoc) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("sub_index_2", "aa", "sub_attr1=1", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false, {0, 1}, true, {{0}, {3}});
}

END_HA3_NAMESPACE();
