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
#include <ha3/turing/variant/AggregatorVariant.h>
#include <ha3/turing/variant/LayerMetasVariant.h>
#include <ha3/search/LayerMetas.h>
#include <basic_ops/ops/test/OpTestBase.h>
#include <ha3/turing/ops/agg/PrepareExpressionResourceOp.h>
#include <ha3/turing/ops/agg/SeekIteratorPrepareOp.h>
#include <ha3/turing/ops/agg/AggPrepareOp.h>
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


class SeekAndAggOpSubDocTest : public OpTestBase {
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
                NodeDefBuilder("mySeekAndAggOpSubDoc", "SeekAndAggOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Attr("batch_size", batchSize)
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(const vector<int> &groupCounts) {
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
    }

private:
    RequestPtr prepareRequest(const string &indexStr,
                              const string &termStr,
                              const string &aggStr,
                              const string &aggType,
                              const string &filterStr = "",
                              const string &pkFilterStr = "",
                              bool hasSubDoc = false,
                              uint32_t rankSize = 0)
    {
        RequestPtr request(new Request(&_pool));
        Term term;
        term.setWord(termStr.c_str());
        term.setIndexName(indexStr.c_str());
        TermQuery *termQuery = new TermQuery(term, "");
        QueryClause *queryClause = new QueryClause(termQuery);
        request->setQueryClause(queryClause);
        ConfigClause * configClause = new ConfigClause();
        configClause->setRankSize(rankSize);
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
        if (!aggStr.empty()) {
            AggregateClause *aggClause = new AggregateClause();
            AggregateDescription *aggDescription = new AggregateDescription();
            SyntaxParser parser;
            SyntaxExpr *expr =  parser.parseSyntax(aggStr);
            VirtualAttributes virtualAttributes;
            SyntaxExprValidator validator(_sessionResource->tableInfo->getAttributeInfos(),
                    virtualAttributes, hasSubDoc);
            validator.validate(expr);
            aggDescription->setGroupKeyExpr(expr);
            AggFunDescription *func = new AggFunDescription(aggType, NULL);
            aggDescription->appendAggFunDescription(func);
            aggClause->addAggDescription(aggDescription);
            request->setAggregateClause(aggClause);
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

    AggregatorVariant prepareAggVariant(RequestPtr request, const SeekIteratorVariant &itVariant) {
        auto queryResource = getQueryResource().get();
        string mainTableName = _sessionResource->bizInfo._itemTableName;
        ExpressionResourcePtr resource = itVariant._expressionResource;
        AggregatorPtr agg = AggPrepareOp::createAggregator(resource, queryResource);
        AggregatorVariant variant(agg, resource);
        return variant;
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, SeekAndAggOpSubDocTest);

TEST_F(SeekAndAggOpSubDocTest, testGetAllSubDocs) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 5}};
    auto request = prepareRequest("index_2", "a", "sub_attr1", "count", "", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AggregatorVariant aggVariant = prepareAggVariant(request, iteratorVariant);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({3, 4, 1});
}

TEST_F(SeekAndAggOpSubDocTest, testGetAllSubDocs_Batch) {
    makeOp(2);
    vector<pair<int32_t, int32_t> > ranges = {{0, 5}};
    auto request = prepareRequest("index_2", "a", "sub_attr1", "count", "", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AggregatorVariant aggVariant = prepareAggVariant(request, iteratorVariant);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({3, 4, 1});
}

TEST_F(SeekAndAggOpSubDocTest, testGetAllSubDocsWithFilter) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 5}};
    auto request = prepareRequest("index_2", "a", "sub_attr1", "count", "attr2=1", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AggregatorVariant aggVariant = prepareAggVariant(request, iteratorVariant);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 1, 1});
}

TEST_F(SeekAndAggOpSubDocTest, testGetAllSubDocsWithFilter_RankSize) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 5}};
    auto request = prepareRequest("index_2", "a", "sub_attr1", "count", "attr2=1", "", true, 3);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AggregatorVariant aggVariant = prepareAggVariant(request, iteratorVariant);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 1, 1});
}

TEST_F(SeekAndAggOpSubDocTest, testGetAllSubDocsWithFilter_RankSize_Batch) {
    makeOp(2);
    vector<pair<int32_t, int32_t> > ranges = {{0, 5}};
    auto request = prepareRequest("index_2", "a", "sub_attr1", "count", "attr2=1", "", true, 3);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AggregatorVariant aggVariant = prepareAggVariant(request, iteratorVariant);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 1, 1});
}

TEST_F(SeekAndAggOpSubDocTest, testGetSeekSubDocs) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("sub_index_2", "abc", "sub_attr1", "count", "", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AggregatorVariant aggVariant = prepareAggVariant(request, iteratorVariant);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({2});
}

TEST_F(SeekAndAggOpSubDocTest, testGetSeekSubDocs_BigBatch) {
    makeOp(5);
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("sub_index_2", "abc", "sub_attr1", "count", "", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AggregatorVariant aggVariant = prepareAggVariant(request, iteratorVariant);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({2});
}


TEST_F(SeekAndAggOpSubDocTest, testSeekMainDocWithQuerySubDoc) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("sub_index_2", "aa", "attr1", "count", "", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AggregatorVariant aggVariant = prepareAggVariant(request, iteratorVariant);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 2, 2, 1});
}

TEST_F(SeekAndAggOpSubDocTest, testSeekMainDocWithFilterSubDoc) {
    makeOp();
    vector<pair<int32_t, int32_t> > ranges = {{0, 9}};
    auto request = prepareRequest("sub_index_2", "aa", "attr1", "count", "sub_attr1=1", "", true);
    SeekIteratorVariant iteratorVariant = prepareIteratorVariant(ranges, request);
    AggregatorVariant aggVariant = prepareAggVariant(request, iteratorVariant);
    AddInputFromArray<Variant>(TensorShape({}), {aggVariant});
    AddInputFromArray<Variant>(TensorShape({}), {iteratorVariant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput({1, 1});
}

END_HA3_NAMESPACE();
