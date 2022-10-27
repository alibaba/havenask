#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include "suez/turing/common/CavaConfig.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "suez/turing/expression/util/TableInfoConfigurator.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "ha3/turing/ops/test/Ha3OpTestBase.h"
#include "ha3/turing/ops/Ha3SeekParaMergeOp.h"
#include "ha3/turing/ops/test/MockCavaJitWrapper.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/rank/RankProfileManager.h"
#include "ha3/qrs/RequestValidateProcessor.h"
#include "suez/turing/common/FileUtil.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/common/test/ResultConstructor.h"
#include "ha3/turing/variant/AggregateResultsVariant.h"
#include "ha3_sdk/testlib/index/FakeIndexPartition.h"
#include "ha3_sdk/testlib/index/FakeIndexPartitionReader.h"
#include "ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h"

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;
using namespace autil;
using namespace autil::mem_pool;
using namespace build_service::plugin;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(func_expression);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(qrs);

BEGIN_HA3_NAMESPACE(turing);

class Ha3SeekParaMergeOpTest : public Ha3OpTestBase {
private:
    RequestPtr _request;
    string _defaultGroupExpr;
    vector<SessionMetricsCollectorPtr> _metricsCollectors;
public:
    void SetUp() override {
        OpTestBase::SetUp();
        _defaultGroupExpr = "groupExprStr";
    }

    void TearDown() override {
        _metricsCollectors.clear();
        OpTestBase::TearDown();
    }

private:
    void initSearcherQueryResource() {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
                getQueryResource());
        ASSERT_TRUE(searcherQueryResource != nullptr);
        FakeIndex fakeIndex;
        fakeIndex.versionId = 1;
        fakeIndex.indexes["pk"] = "1:1,2:2,3:3,4:4,5:5,6:6;\n";
        IndexPartitionReaderWrapperPtr indexWraper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        TableMem2IdMap emptyTableMem2IdMap;
        PartitionReaderSnapshotPtr snapShot(
                new PartitionReaderSnapshot(&emptyTableMem2IdMap,
                        &emptyTableMem2IdMap, &emptyTableMem2IdMap,
                        vector<IndexPartitionReaderInfo>()));
        searcherQueryResource->indexPartitionReaderWrapper = indexWraper;
        searcherQueryResource->setIndexSnapshot(snapShot);
    }

    void initSearcherSessionResource() {
        auto searcherSessionResource =
            dynamic_pointer_cast<SearcherSessionResource>(_sessionResource);
        ASSERT_TRUE(searcherSessionResource != nullptr);
        SearcherResourcePtr searcherResource(new HA3_NS(service)::SearcherResource);
        RankProfileManagerPtr rankProfileMgrPtr;
        initRankProfileManager(rankProfileMgrPtr, 100, 10);
        searcherResource->setRankProfileManager(rankProfileMgrPtr);
        searcherResource->setPartCount(1);
        searcherSessionResource->searcherResource = searcherResource;
    }

    void initRankProfileManager(RankProfileManagerPtr &rankProfileMgrPtr,
                                uint32_t rankSize, uint32_t rerankSize) {
        PlugInManagerPtr nullPtr;
        rankProfileMgrPtr.reset(new RankProfileManager(nullPtr));
        ScorerInfo scorerInfo;
        scorerInfo.scorerName = "DefaultScorer";
        scorerInfo.rankSize = rankSize;
        RankProfile *rankProfile = new RankProfile(DEFAULT_RANK_PROFILE);
        rankProfile->addScorerInfo(scorerInfo);
        scorerInfo.rankSize = rerankSize;
        rankProfile->addScorerInfo(scorerInfo); //two scorer
        rankProfileMgrPtr->addRankProfile(rankProfile);

        CavaPluginManagerPtr cavaPluginManagerPtr;
        config::ResourceReaderPtr resourceReader;
        rankProfileMgrPtr->init(resourceReader, cavaPluginManagerPtr, NULL);
    }

    RequestPtr prepareRequest() {
        RequestPtr requestPtr(new Request);
        addAggregateDescription(requestPtr.get(), _defaultGroupExpr);
        requestPtr->setConfigClause(new ConfigClause());
        return requestPtr;
    }

    Ha3RequestVariant fakeHa3Request() {
        _request = prepareRequest();
        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

    SeekResourceVariant fakeResourceVariant() {
        TableInfoPtr tableInfoPtr;
        CavaPluginManagerPtr cavaPluginManagerPtr;
        std::map<size_t, ::cava::CavaJitModulePtr> cavaJitModulesMap;
        SeekResourcePtr seekResource(new SeekResource);
        SessionMetricsCollectorPtr metricsCollector(new SessionMetricsCollector());
        _metricsCollectors.push_back(metricsCollector);
        SearchCommonResourcePtr commonResource(
                new SearchCommonResource(&_pool, tableInfoPtr,
                        metricsCollector.get(),
                        nullptr, nullptr, nullptr,cavaPluginManagerPtr,
                        _request.get(), nullptr, cavaJitModulesMap));
        seekResource->commonResource = commonResource;
        FakeIndex fakeIndex;
        fakeIndex.versionId = 1;
        fakeIndex.indexes["pk"] = "1:1,2:2,3:3,4:4,5:5,6:6;\n";
        IndexPartitionReaderWrapperPtr indexWraper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        TableMem2IdMap emptyTableMem2IdMap;
        PartitionReaderSnapshotPtr snapShot(
                new PartitionReaderSnapshot(&emptyTableMem2IdMap,
                        &emptyTableMem2IdMap, &emptyTableMem2IdMap,
                        vector<IndexPartitionReaderInfo>()));
        SearchPartitionResourcePtr partitionResource(
                new SearchPartitionResource(*commonResource, _request.get(),
                    indexWraper, snapShot));
        seekResource->partitionResource = partitionResource;
        SeekResourceVariant variant(seekResource);
        return variant;
    }

    InnerResultVariant fakeInnerResultVariant(const string &resultStr,
            Ha3MatchDocAllocatorPtr allocator = Ha3MatchDocAllocatorPtr())
    {
        InnerSearchResult innerResult = makeInnerResult(resultStr, allocator);
        InnerResultVariant variant(innerResult);
        return variant;
    }

    void addAggregateDescription(common::Request *request,
                                 const std::string &groupExprStr) {
        AggregateClause *aggClause = request->getAggregateClause();
        if (NULL == aggClause) {
            aggClause = new AggregateClause();
            request->setAggregateClause(aggClause);
            aggClause = request->getAggregateClause();
        }

        AggregateDescription *des = new AggregateDescription(groupExprStr);
        SyntaxExpr *syntax = new AtomicSyntaxExpr(
                groupExprStr, vt_int32, ATTRIBUTE_NAME);
        des->setGroupKeyExpr(syntax);
        aggClause->addAggDescription(des);
    }

    void aggregateResultConstruct(
            AggregateResults &aggregateResults,
            const string &funNames,
            const string &slabValue,
            autil::mem_pool::Pool *pool,
            const string &groupExprStr = "groupExprStr",
            const string &funParameters = "")
    {
        AggregateResultPtr aggResultPtr(new AggregateResult(groupExprStr));
        matchdoc::MatchDocAllocatorPtr allocatorPtr(new matchdoc::MatchDocAllocator(pool));
        aggResultPtr->setMatchDocAllocator(allocatorPtr);

        //decalre all the aggregate function's reference
        StringTokenizer funNamesTokenizer(funNames, ",",
                StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        int i = 0;
        auto groupKeyRef = allocatorPtr->declare<string>(
                GROUP_KEY_REF, SL_CACHE);
        for (StringTokenizer::Iterator it = funNamesTokenizer.begin();
             it != funNamesTokenizer.end(); ++it, ++i)
        {
            aggResultPtr->addAggFunName(*it);
            allocatorPtr->declare<int64_t>(*it + StringUtil::toString(i), SL_CACHE);
        }

        if (funParameters != "") {
            StringTokenizer funParametersTokenizer(funParameters, ",", StringTokenizer::TOKEN_TRIM);
            for (StringTokenizer::Iterator it = funParametersTokenizer.begin();
                 it != funParametersTokenizer.end(); ++it)
            {
                aggResultPtr->addAggFunParameter(*it);
            }
        }

        //fill the value into VariableSlab
        StringTokenizer slabValueTokenizer(slabValue, "#",
                StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        for (StringTokenizer::Iterator it = slabValueTokenizer.begin();
             it != slabValueTokenizer.end(); ++it, ++i)
        {
            StringTokenizer slabValueTokens(*it, ",",
                    StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
            assert(funNamesTokenizer.getNumTokens() + 1 == slabValueTokens.getNumTokens());
            matchdoc::MatchDoc slab = allocatorPtr->allocate();
            auto referenceVec = allocatorPtr->getAllNeedSerializeReferences(
                    SL_NONE + 1);
            int32_t value;
            int32_t j = 0;
            for (auto referIt = referenceVec.begin() + 1;
                 referIt != referenceVec.end(); ++referIt, ++j) {
                StringUtil::strToInt32(slabValueTokens[j+1].c_str(), value);
                auto refer = dynamic_cast<const matchdoc::Reference<int64_t>*>(*referIt);
                refer->set(slab, (int64_t)value);
            }
            groupKeyRef->set(slab, slabValueTokens[0]);
            aggResultPtr->addAggFunResults(slab);
        }
        aggResultPtr->constructGroupValueMap();
        aggregateResults.push_back(aggResultPtr);
    }

    void makeMatchDocs(PoolVector<MatchDoc> &matchDocVec,
                       Ha3MatchDocAllocatorPtr &matchDocAllocator,
                       const string &fieldsDesc, const string &docsDesc) {
        // field = pk:uint32,ref1:string,ref2:int32_t,ref3:int64_t
        // val = 12,"abc",33,55;22,"cc",1,3;
        matchDocVec.clear();
        vector<ReferenceBase *> fields;
        const vector<string> &fieldsVec = StringUtil::split(fieldsDesc, ",");
        for (const auto &oneFieldPair : fieldsVec) {
            const vector<string> &fpar = StringUtil::split(oneFieldPair, ":");
            assert(fpar.size() == 2);
            if (fpar[0] == "pk") {
                auto ref = matchDocAllocator->declare<uint64_t>(PRIMARYKEY_REF);
                fields.push_back(ref);
            } else if (fpar[1] == "int32_t") {
                auto ref = matchDocAllocator->declare<int32_t>(fpar[0]);
                fields.push_back(ref);
            } else if (fpar[1] == "string") {
                auto ref = matchDocAllocator->declare<string>(fpar[0]);
                fields.push_back(ref);
            }
        }

        const vector<string> &valuesVec = StringUtil::split(docsDesc, ";");
        int32_t docid = 0;
        for (const auto &oneDocVals : valuesVec) {
            const vector<string> &one = StringUtil::split(oneDocVals, ",");
            assert(one.size() == fields.size());
            MatchDoc doc = matchDocAllocator->allocate(docid++);
            for (size_t i = 0; i < one.size(); i++) {
                auto builtinType = fields[i]->getValueType().getBuiltinType();
                if (bt_int32 == builtinType) {
                    ((Reference<int32_t> *)fields[i])
                        ->set(doc, StringUtil::fromString<int32_t>(one[i]));
                } else if (bt_string == builtinType) {
                    ((Reference<string> *)fields[i])
                        ->set(doc, StringUtil::fromString<string>(one[i]));
                } else if (bt_uint64 == builtinType) {
                    ((Reference<uint64_t> *)fields[i])
                        ->set(doc, StringUtil::fromString<uint64_t>(one[i]));
                }
            }
            matchDocVec.push_back(doc);
        }
    }

    InnerSearchResult makeInnerResult(const string &resultStr,
            Ha3MatchDocAllocatorPtr allocator = Ha3MatchDocAllocatorPtr())
    {
        //result format: "fieldDesc| docDesc | totalMatchdocs|
        // actualMatchDocs| extraRankCount| aggResult"
        const vector<string> &resultVec =
            StringUtil::split(resultStr, "|", false);
        assert(size_t(5) <= resultVec.size());
        InnerSearchResult result(&_pool);
        if (allocator) {
            result.matchDocAllocatorPtr = allocator;
        } else {
            result.matchDocAllocatorPtr.reset(
                    new Ha3MatchDocAllocator(&_pool));
        }
        // result.matchDocAllocatorPtr->initPhaseOneInfoReferences(
        //         common::pob_primarykey64_flag);
        makeMatchDocs(result.matchDocVec, result.matchDocAllocatorPtr,
                      resultVec[0], resultVec[1]);
        result.totalMatchDocs = StringUtil::fromString<int32_t>(resultVec[2]);
        result.actualMatchDocs = StringUtil::fromString<int32_t>(resultVec[3]);
        result.extraRankCount = StringUtil::fromString<int32_t>(resultVec[4]);

        if (6 == resultVec.size()) {
            const vector<string> &aggStrVec = StringUtil::split(resultVec[5], ";");
            assert(size_t(2) == aggStrVec.size());
            string aggFunDescStr = aggStrVec[0];
            string aggFunValue = aggStrVec[1];
            result.aggResultsPtr.reset(new AggregateResults);
            aggregateResultConstruct(*result.aggResultsPtr, aggFunDescStr,
                    aggFunValue, &_pool, _defaultGroupExpr);
        }
        return result;
    }

    void prepareSeekParaMergeOp(const int32_t wayCount,
                                Ha3SeekParaMergeOp *&outOp)
    {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3SeekParaMergeOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(wayCount, DT_VARIANT))
                .Input(FakeInput(wayCount, DT_VARIANT))
                .Input(FakeInput(wayCount, DT_BOOL))
                .Attr("N", wayCount)
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
        outOp = dynamic_cast<Ha3SeekParaMergeOp *>(kernel_.get());
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3SeekParaMergeOpTest);

void printInnerResult(const string &resultName,
                      const InnerSearchResult &result)
{
    cout << "------" << resultName << "------" << endl;
    auto refVec = result.matchDocAllocatorPtr->
                  getAllNeedSerializeReferences(0);
    for (size_t i = 0; i < result.matchDocVec.size(); i++) {
        auto doc = result.matchDocVec[i];
        cout << "docId: " << doc.getDocId()
             << ", index: " << doc.getIndex();
        for (auto ref : refVec) {
            cout << ", " << ref->getName() << ": "
                 << ref->toString(doc);
        }
        cout << endl;
    }
    cout << "doc: " << result.matchDocVec.size()
         << ", totalMatchDocs: " << result.totalMatchDocs
         << ", actualMatchDocs: " << result.actualMatchDocs
         << ", extraRankCount: " << result.extraRankCount << endl;
}

template <typename T>
void assertMatchDocs(const string& expectStr,
                     const MatchDocAllocator *matchDocAllocatorPtr,
                     const T &matchDocVec)
{
    //"0,1,aaa;1,2,bbb;3,3,ccc;4,4,ddd;5,5,eee";
    const vector<string> &valuesVec = StringUtil::split(expectStr, ";");
    auto refVec = matchDocAllocatorPtr->getAllNeedSerializeReferences(0);
    for (size_t i = 0; i < matchDocVec.size(); i++) {
        auto doc = matchDocVec[i];
        const vector<string> &expect = StringUtil::split(valuesVec[i], ",");
        ASSERT_EQ(expect.size(), refVec.size());
        for (size_t idx = 0; idx < refVec.size(); ++idx) {
            ASSERT_EQ(expect[idx], refVec[idx]->toString(doc));
        }
    }
}

void assertMatchDocs(const string& expectStr,
                     const InnerSearchResult& result)
{
    assertMatchDocs<PoolVector<matchdoc::MatchDoc> >(expectStr,
                                (MatchDocAllocator*)
                                (result.matchDocAllocatorPtr.get()),
                                result.matchDocVec);
}

void assertAggResult(const string& expectStr, const AggregateResults& results) {
    //expectStr: key1,4444,111#key2,1234,222#key3,323,54|user1,123#user2,456"
    const vector<string> &aggResultsStrVec = StringUtil::split(expectStr, "|");
    ASSERT_EQ(aggResultsStrVec.size(), results.size());
    for (size_t index=0; index<aggResultsStrVec.size(); ++index) {
        const vector<string> &valuesVec = StringUtil::split(
                aggResultsStrVec[index], "#");
        AggregateResultPtr aggResultPtr = results[index];
        ASSERT_TRUE(aggResultPtr);
        aggResultPtr->constructGroupValueMap();
        ASSERT_EQ(valuesVec.size(), aggResultPtr->getAggExprValueCount());
        ASSERT_EQ(valuesVec.size(), aggResultPtr->getAggExprValueMap().size());
        for (size_t i = 0; i < valuesVec.size(); i++) {
            const vector<string> &oneKey = StringUtil::split(valuesVec[i], ",");
            const string &keyName = oneKey[0];
            for (size_t j = 1; j < oneKey.size(); j++) {
                ASSERT_EQ(StringUtil::fromString<int64_t>(oneKey[j]),
                        aggResultPtr->getAggFunResult<int64_t>(keyName, j-1, 0));
            }
        }
    }
}

TEST_F(Ha3SeekParaMergeOpTest, testMergeMatchDocs) {
    // field = pk:uint32,ref1:string,ref2:int32_t,ref3:int64_t
    // val = 12,"abc",33,55;22,"cc",1,3;
    // result format: "fieldDesc| docDesc| totalMatchdocs|
    // actualMatchDocs| extraRankCount| aggResult"

    {
        string str1(
            "pk:uint64_t,ref1:int32_t,ref2:string|0,1,aaa;1,2,bbb|10|2|2");
        string str2("pk:uint64_t,ref1:int32_t,ref2:string|3,3,ccc;4,4,ddd;5,5,"
                    "eee|20|4|4");
        const InnerSearchResult &result1 = makeInnerResult(str1);
        const InnerSearchResult &result2 = makeInnerResult(str2);
        vector<InnerSearchResult> innerResults;
        innerResults.push_back(result1);
        innerResults.push_back(result2);
        // printInnerResult("111", result1);
        // printInnerResult("222", result2);

        InnerSearchResult mergedResult(&_pool);
        ErrorCode error = ERROR_NONE;
        Ha3SeekParaMergeOp *mergeOp = NULL;
        prepareSeekParaMergeOp(2, mergeOp);
        ASSERT_TRUE(mergeOp != NULL);
        mergeOp->mergeMatchDocs(innerResults, mergedResult, error);
        // printInnerResult("merged", mergedResult);
        ASSERT_EQ(5, mergedResult.matchDocVec.size());
        assertMatchDocs("0,1,aaa;1,2,bbb;3,3,ccc;4,4,ddd;5,5,eee",
                        mergedResult);
        ASSERT_EQ(ERROR_NONE, error);
        ASSERT_EQ(30, mergedResult.totalMatchDocs);
        ASSERT_EQ(6, mergedResult.actualMatchDocs);
        ASSERT_EQ(6, mergedResult.actualMatchDocs);
    }

    // pk not dedup
    {
        string str1(
            "pk:uint64_t,ref1:int32_t,ref2:string|0,1,aaa;1,2,bbb|10|2|2");
        string str2("pk:uint64_t,ref1:int32_t,ref2:string|3,3,ccc;4,4,ddd;1,5,"
                    "eee|20|4|4");
        const InnerSearchResult &result1 = makeInnerResult(str1);
        const InnerSearchResult &result2 = makeInnerResult(str2);
        vector<InnerSearchResult> innerResults;
        innerResults.push_back(result1);
        innerResults.push_back(result2);
        // printInnerResult("111", result1);
        // printInnerResult("222", result2);

        InnerSearchResult mergedResult(&_pool);
        ErrorCode error = ERROR_NONE;
        Ha3SeekParaMergeOp *mergeOp = NULL;
        prepareSeekParaMergeOp(2, mergeOp);
        ASSERT_TRUE(mergeOp != NULL);
        mergeOp->mergeMatchDocs(innerResults, mergedResult, error);
        // printInnerResult("merged", mergedResult);
        ASSERT_EQ(5, mergedResult.matchDocVec.size());
        assertMatchDocs("0,1,aaa;1,2,bbb;3,3,ccc;4,4,ddd;1,5,eee", mergedResult);
        ASSERT_EQ(30, mergedResult.totalMatchDocs);
        ASSERT_EQ(6, mergedResult.actualMatchDocs);
        ASSERT_EQ(6, mergedResult.actualMatchDocs);
    }
}

TEST_F(Ha3SeekParaMergeOpTest, testMergeAggResults) {
    InnerSearchResult result1(&_pool);
    InnerSearchResult result2(&_pool);
    result1.aggResultsPtr.reset(new AggregateResults);
    result2.aggResultsPtr.reset(new AggregateResults);
    AggregateResults &aggResults1 = *result1.aggResultsPtr;
    string groupExprStr = "user_id";
    aggregateResultConstruct(aggResults1, "sum,count",
                             "user1, 4444, 111#user2, 1234, 222", &_pool,
                             groupExprStr);

    AggregateResults &aggResults2 = *result2.aggResultsPtr;
    aggregateResultConstruct(aggResults2, "sum,count",
                             "user1, 4443, 111#user3, 1234, 222", &_pool,
                             groupExprStr);

    Ha3SeekParaMergeOp *mergeOp = NULL;
    prepareSeekParaMergeOp(2, mergeOp);
    AggregateResults mergedAggResults;
    vector<InnerSearchResult> innerResults;
    innerResults.push_back(result1);
    innerResults.push_back(result2);

    RequestPtr request(new Request(&_pool));
    addAggregateDescription(request.get(), groupExprStr);
    mergeOp->mergeAggResults(mergedAggResults, innerResults,
                             request->getAggregateClause(), &_pool);

    ASSERT_EQ((size_t)1, mergedAggResults.size());
    assertAggResult("user1,8887,222#user2,1234,222#user3,1234,222",
                    mergedAggResults);
}

TEST_F(Ha3SeekParaMergeOpTest, testOpSimple) {
    Ha3SeekParaMergeOp *mergeOp;
    prepareSeekParaMergeOp(2, mergeOp);
    initSearcherQueryResource();
    initSearcherSessionResource();
    Variant variant = fakeHa3Request();

    AddInputFromArray<Variant>(TensorShape({}), {variant});
    SeekResourceVariant resourceVariant1 = fakeResourceVariant();
    SeekResourceVariant resourceVariant2 = fakeResourceVariant();
    AddInputFromArray<Variant>(TensorShape({}), {resourceVariant1});
    AddInputFromArray<Variant>(TensorShape({}), {resourceVariant2});

    string str1(
            "pk:uint64_t,ref1:int32_t,ref2:string|0,1,aaa;1,2,bbb"
            "|10|2|2"
            "|sum,count;user1, 4444, 111#user2, 1234, 222");
    string str2(
            "pk:uint64_t,ref1:int32_t,ref2:string|3,3,ccc;4,4,ddd;5,5,eee"
            "|20|4|4"
            "|sum,count;user1, 4443, 111#user3, 1234, 222");

    auto allocator1 = resourceVariant1.getSeekResource()
                      ->commonResource->matchDocAllocator;
    auto allocator2 = resourceVariant2.getSeekResource()
                      ->commonResource->matchDocAllocator;
    const Variant &seekResultVariant1 = fakeInnerResultVariant(str1, allocator1);
    const Variant &seekResultVariant2 = fakeInnerResultVariant(str2, allocator2);
    AddInputFromArray<Variant>(TensorShape({}), {seekResultVariant1});
    AddInputFromArray<Variant>(TensorShape({}), {seekResultVariant2});

    AddInputFromArray<bool>(TensorShape({}), {false});
    AddInputFromArray<bool>(TensorShape({}), {false});
    TF_ASSERT_OK(RunOpKernel());
    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    ASSERT_EQ(5u, matchDocsVariant->getMatchDocs().size());
    assertMatchDocs<vector<matchdoc::MatchDoc> >(
            "0,1,aaa;1,2,bbb;3,3,ccc;4,4,ddd;5,5,eee",
            matchDocsVariant->getAllocator().get(),
            matchDocsVariant->getMatchDocs());

    auto pExtraRankCountOutputTensor = GetOutput(1);
    ASSERT_TRUE(pExtraRankCountOutputTensor != nullptr);
    auto basicValue1 = pExtraRankCountOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(6u, basicValue1);

    auto pTotalMatchDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pTotalMatchDocsOutputTensor != nullptr);
    auto basicValue2 = pTotalMatchDocsOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(30u, basicValue2);

    auto pActualMatchDocsOutputTensor = GetOutput(3);
    ASSERT_TRUE(pActualMatchDocsOutputTensor != nullptr);
    auto basicValue3 = pActualMatchDocsOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(6u, basicValue3);

    auto pHa3AggResultsOutputTensor = GetOutput(4);
    ASSERT_TRUE(pHa3AggResultsOutputTensor != nullptr);
    auto aggResultsVariant = pHa3AggResultsOutputTensor->scalar<Variant>()()
                             .get<AggregateResultsVariant>();
    ASSERT_TRUE(aggResultsVariant != nullptr);
    assertAggResult("user1,8887,222#user2,1234,222#user3,1234,222",
                    *(aggResultsVariant->getAggResults()));

    auto pErrorResultsOutputTensor = GetOutput(5);
    ASSERT_TRUE(pErrorResultsOutputTensor == nullptr);
}

TEST_F(Ha3SeekParaMergeOpTest, testOpOneResultEmpty) {
    Ha3SeekParaMergeOp *mergeOp;
    prepareSeekParaMergeOp(2, mergeOp);
    initSearcherQueryResource();
    initSearcherSessionResource();
    Variant variant = fakeHa3Request();

    AddInputFromArray<Variant>(TensorShape({}), {variant});
    SeekResourceVariant resourceVariant1 = fakeResourceVariant();
    SeekResourceVariant resourceVariant2 = fakeResourceVariant();
    AddInputFromArray<Variant>(TensorShape({}), {resourceVariant1});
    AddInputFromArray<Variant>(TensorShape({}), {resourceVariant2});

    string str2(
            "pk:uint64_t,ref1:int32_t,ref2:string|3,3,ccc;4,4,ddd;5,5,eee"
            "|20|4|4"
            "|sum,count;user1, 4443, 111#user3, 1234, 222");

    InnerSearchResult innerResult(&_pool);
    InnerResultVariant seekResultVariant1 = InnerResultVariant(innerResult);
    auto allocator2 = resourceVariant2.getSeekResource()
                      ->commonResource->matchDocAllocator;
    const Variant &seekResultVariant2 = fakeInnerResultVariant(str2, allocator2);
    AddInputFromArray<Variant>(TensorShape({}), {seekResultVariant1});
    AddInputFromArray<Variant>(TensorShape({}), {seekResultVariant2});

    AddInputFromArray<bool>(TensorShape({}), {false});
    AddInputFromArray<bool>(TensorShape({}), {false});
    TF_ASSERT_OK(RunOpKernel());
    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    ASSERT_EQ(3u, matchDocsVariant->getMatchDocs().size());
    assertMatchDocs<vector<matchdoc::MatchDoc> >(
            "3,3,ccc;4,4,ddd;5,5,eee",
            matchDocsVariant->getAllocator().get(),
            matchDocsVariant->getMatchDocs());

    auto pExtraRankCountOutputTensor = GetOutput(1);
    ASSERT_TRUE(pExtraRankCountOutputTensor != nullptr);
    auto basicValue1 = pExtraRankCountOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(4u, basicValue1);

    auto pTotalMatchDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pTotalMatchDocsOutputTensor != nullptr);
    auto basicValue2 = pTotalMatchDocsOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(20u, basicValue2);

    auto pActualMatchDocsOutputTensor = GetOutput(3);
    ASSERT_TRUE(pActualMatchDocsOutputTensor != nullptr);
    auto basicValue3 = pActualMatchDocsOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(4u, basicValue3);

    auto pHa3AggResultsOutputTensor = GetOutput(4);
    ASSERT_TRUE(pHa3AggResultsOutputTensor != nullptr);
    auto aggResultsVariant = pHa3AggResultsOutputTensor->scalar<Variant>()()
                             .get<AggregateResultsVariant>();
    ASSERT_TRUE(aggResultsVariant != nullptr);
    assertAggResult("user1,4443,111#user3,1234,222",
                    *(aggResultsVariant->getAggResults()));

    auto pErrorResultsOutputTensor = GetOutput(5);
    ASSERT_TRUE(pErrorResultsOutputTensor == nullptr);
}

TEST_F(Ha3SeekParaMergeOpTest, testOpOneResultNoMatchDocs) {
    Ha3SeekParaMergeOp *mergeOp;
    prepareSeekParaMergeOp(2, mergeOp);
    initSearcherQueryResource();
    initSearcherSessionResource();
    Variant variant = fakeHa3Request();

    AddInputFromArray<Variant>(TensorShape({}), {variant});
    SeekResourceVariant resourceVariant1 = fakeResourceVariant();
    SeekResourceVariant resourceVariant2 = fakeResourceVariant();
    AddInputFromArray<Variant>(TensorShape({}), {resourceVariant1});
    AddInputFromArray<Variant>(TensorShape({}), {resourceVariant2});

    string str1(
            "pk:uint64_t,ref1:int32_t,ref2:string|"
            "|10|2|2"
            "|sum,count;user1, 4444, 111#user2, 1234, 222");
    string str2(
            "pk:uint64_t,ref1:int32_t,ref2:string|3,3,ccc;4,4,ddd;5,5,eee"
            "|20|4|4"
            "|sum,count;user1, 4443, 111#user3, 1234, 222");
    auto allocator1 = resourceVariant1.getSeekResource()
                      ->commonResource->matchDocAllocator;
    auto allocator2 = resourceVariant2.getSeekResource()
                      ->commonResource->matchDocAllocator;
    const Variant &seekResultVariant1 = fakeInnerResultVariant(str1, allocator1);
    const Variant &seekResultVariant2 = fakeInnerResultVariant(str2, allocator2);
    AddInputFromArray<Variant>(TensorShape({}), {seekResultVariant1});
    AddInputFromArray<Variant>(TensorShape({}), {seekResultVariant2});

    AddInputFromArray<bool>(TensorShape({}), {false});
    AddInputFromArray<bool>(TensorShape({}), {false});
    TF_ASSERT_OK(RunOpKernel());
    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    ASSERT_EQ(3u, matchDocsVariant->getMatchDocs().size());
    assertMatchDocs<vector<matchdoc::MatchDoc> >(
            "3,3,ccc;4,4,ddd;5,5,eee",
            matchDocsVariant->getAllocator().get(),
            matchDocsVariant->getMatchDocs());

    auto pExtraRankCountOutputTensor = GetOutput(1);
    ASSERT_TRUE(pExtraRankCountOutputTensor != nullptr);
    auto basicValue1 = pExtraRankCountOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(6u, basicValue1);

    auto pTotalMatchDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pTotalMatchDocsOutputTensor != nullptr);
    auto basicValue2 = pTotalMatchDocsOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(30u, basicValue2);

    auto pActualMatchDocsOutputTensor = GetOutput(3);
    ASSERT_TRUE(pActualMatchDocsOutputTensor != nullptr);
    auto basicValue3 = pActualMatchDocsOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(6u, basicValue3);

    auto pHa3AggResultsOutputTensor = GetOutput(4);
    ASSERT_TRUE(pHa3AggResultsOutputTensor != nullptr);
    auto aggResultsVariant = pHa3AggResultsOutputTensor->scalar<Variant>()()
                             .get<AggregateResultsVariant>();
    ASSERT_TRUE(aggResultsVariant != nullptr);
    assertAggResult("user1,8887,222#user2,1234,222#user3,1234,222",
                    *(aggResultsVariant->getAggResults()));

    auto pErrorResultsOutputTensor = GetOutput(5);
    ASSERT_TRUE(pErrorResultsOutputTensor == nullptr);
}

TEST_F(Ha3SeekParaMergeOpTest, testOpAllResultsNoMatchDocs) {
    Ha3SeekParaMergeOp *mergeOp;
    prepareSeekParaMergeOp(2, mergeOp);
    initSearcherQueryResource();
    initSearcherSessionResource();
    Variant variant = fakeHa3Request();

    AddInputFromArray<Variant>(TensorShape({}), {variant});
    SeekResourceVariant resourceVariant1 = fakeResourceVariant();
    SeekResourceVariant resourceVariant2 = fakeResourceVariant();
    AddInputFromArray<Variant>(TensorShape({}), {resourceVariant1});
    AddInputFromArray<Variant>(TensorShape({}), {resourceVariant2});

    string str1(
            "pk:uint64_t,ref1:int32_t,ref2:string|"
            "|10|2|2"
            "|sum,count;user1, 4444, 111#user2, 1234, 222");
    string str2(
            "pk:uint64_t,ref1:int32_t,ref2:string|"
            "|20|4|4"
            "|sum,count;user1, 4443, 111#user3, 1234, 222");
    auto allocator1 = resourceVariant1.getSeekResource()
                      ->commonResource->matchDocAllocator;
    auto allocator2 = resourceVariant2.getSeekResource()
                      ->commonResource->matchDocAllocator;
    const Variant &seekResultVariant1 = fakeInnerResultVariant(str1, allocator1);
    const Variant &seekResultVariant2 = fakeInnerResultVariant(str2, allocator2);
    AddInputFromArray<Variant>(TensorShape({}), {seekResultVariant1});
    AddInputFromArray<Variant>(TensorShape({}), {seekResultVariant2});

    AddInputFromArray<bool>(TensorShape({}), {false});
    AddInputFromArray<bool>(TensorShape({}), {false});
    TF_ASSERT_OK(RunOpKernel());
    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    ASSERT_EQ(0u, matchDocsVariant->getMatchDocs().size());
    assertMatchDocs<vector<matchdoc::MatchDoc> >(
            "",
            matchDocsVariant->getAllocator().get(),
            matchDocsVariant->getMatchDocs());

    auto pExtraRankCountOutputTensor = GetOutput(1);
    ASSERT_TRUE(pExtraRankCountOutputTensor != nullptr);
    auto basicValue1 = pExtraRankCountOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(6u, basicValue1);

    auto pTotalMatchDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pTotalMatchDocsOutputTensor != nullptr);
    auto basicValue2 = pTotalMatchDocsOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(30u, basicValue2);

    auto pActualMatchDocsOutputTensor = GetOutput(3);
    ASSERT_TRUE(pActualMatchDocsOutputTensor != nullptr);
    auto basicValue3 = pActualMatchDocsOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(6u, basicValue3);

    auto pHa3AggResultsOutputTensor = GetOutput(4);
    ASSERT_TRUE(pHa3AggResultsOutputTensor != nullptr);
    auto aggResultsVariant = pHa3AggResultsOutputTensor->scalar<Variant>()()
                             .get<AggregateResultsVariant>();
    ASSERT_TRUE(aggResultsVariant != nullptr);
    assertAggResult("user1,8887,222#user2,1234,222#user3,1234,222",
                    *(aggResultsVariant->getAggResults()));

    auto pErrorResultsOutputTensor = GetOutput(5);
    ASSERT_TRUE(pErrorResultsOutputTensor == nullptr);
}

TEST_F(Ha3SeekParaMergeOpTest, testOpAllResultsError){
    Ha3SeekParaMergeOp *mergeOp;
    prepareSeekParaMergeOp(2, mergeOp);
    initSearcherQueryResource();
    Variant variant = fakeHa3Request();

    AddInputFromArray<Variant>(TensorShape({}), {variant});
    Variant resourceVariant1 = fakeResourceVariant();
    Variant resourceVariant2 = fakeResourceVariant();
    AddInputFromArray<Variant>(TensorShape({}), {resourceVariant1});
    AddInputFromArray<Variant>(TensorShape({}), {resourceVariant2});

    string str1(
            "pk:uint64_t,ref1:int32_t,ref2:string|0,1,aaa;1,2,bbb"
            "|10|2|2"
            "|sum,count;key1, 4444, 111#key2, 1234, 222");
    string str2(
            "pk:uint64_t,ref1:int32_t,ref2:string|3,3,ccc;4,4,ddd;5,5,eee"
            "|20|4|4"
            "|sum,count;key1, 4443, 111#key3, 1234, 222");

    const Variant &seekResultVariant1 = fakeInnerResultVariant(str1);
    const Variant &seekResultVariant2 = fakeInnerResultVariant(str2);
    AddInputFromArray<Variant>(TensorShape({}), {seekResultVariant1});
    AddInputFromArray<Variant>(TensorShape({}), {seekResultVariant2});

    AddInputFromArray<bool>(TensorShape({}), {true});
    AddInputFromArray<bool>(TensorShape({}), {true});
    TF_ASSERT_OK(RunOpKernel());

    auto pErrorResultsOutputTensor = GetOutput(5);
    ASSERT_TRUE(pErrorResultsOutputTensor != nullptr);
}

TEST_F(Ha3SeekParaMergeOpTest, testFindFirstNoneEmptyResult) {
    Ha3SeekParaMergeOp *mergeOp;
    prepareSeekParaMergeOp(2, mergeOp);
    {
        vector<InnerSearchResult> innerResults;
        string str1(
                "pk:uint64_t,ref1:int32_t,ref2:string|0,1,aaa|10|2|2");
        string str2("pk:uint64_t,ref1:int32_t,ref2:string|3,3,ccc;4,4,ddd;5,5,"
                    "eee|20|4|4");
        const InnerSearchResult &result1 = makeInnerResult(str1);
        const InnerSearchResult &result2 = makeInnerResult(str2);
        innerResults.push_back(result1);
        innerResults.push_back(result2);

        int32_t index = mergeOp->findFirstNoneEmptyResult(innerResults);
        ASSERT_EQ(0, index);
    }
    {
        vector<InnerSearchResult> innerResults;
        string str1(
                "pk:uint64_t,ref1:int32_t,ref2:string|0,1,aaa;1,2,bbb|10|2|2");
        string str2("pk:uint64_t,ref1:int32_t,ref2:string|3,3,ccc;4,4,ddd;5,5,"
                    "eee|20|4|4");
        const InnerSearchResult &result1 = makeInnerResult(str1);
        const InnerSearchResult &result2 = makeInnerResult(str2);
        innerResults.push_back(result1);
        innerResults.push_back(result2);

        int32_t index = mergeOp->findFirstNoneEmptyResult(innerResults);
        ASSERT_EQ(0, index);
    }
    {
        vector<InnerSearchResult> innerResults;
        string str(
                "pk:uint64_t,ref1:int32_t,ref2:string|0,1,aaa;1,2,bbb|10|2|2");
        InnerSearchResult result1(&_pool);
        InnerSearchResult result2(&_pool);
        result2.matchDocAllocatorPtr.reset(new Ha3MatchDocAllocator(&_pool));
        const InnerSearchResult &result3 = makeInnerResult(str);
        innerResults.push_back(result1);
        innerResults.push_back(result2);
        innerResults.push_back(result3);
        int32_t index = mergeOp->findFirstNoneEmptyResult(innerResults);
        ASSERT_EQ(2, index);
    }
    {
        vector<InnerSearchResult> innerResults;
        InnerSearchResult result1(&_pool);
        InnerSearchResult result2(&_pool);
        result2.matchDocAllocatorPtr.reset(new Ha3MatchDocAllocator(&_pool));
        innerResults.push_back(result1);
        innerResults.push_back(result2);
        int32_t index = mergeOp->findFirstNoneEmptyResult(innerResults);
        ASSERT_EQ(1, index);
    }
    {
        vector<InnerSearchResult> innerResults;
        InnerSearchResult result1(&_pool);
        InnerSearchResult result2(&_pool);
        result1.matchDocAllocatorPtr.reset(new Ha3MatchDocAllocator(&_pool));
        result2.matchDocAllocatorPtr.reset(new Ha3MatchDocAllocator(&_pool));
        innerResults.push_back(result1);
        innerResults.push_back(result2);
        int32_t index = mergeOp->findFirstNoneEmptyResult(innerResults);
        ASSERT_EQ(0, index);
    }
    {
        vector<InnerSearchResult> innerResults;
        InnerSearchResult result1(&_pool);
        InnerSearchResult result2(&_pool);
        innerResults.push_back(result1);
        innerResults.push_back(result2);
        int32_t index = mergeOp->findFirstNoneEmptyResult(innerResults);
        ASSERT_EQ(-1, index);
    }
}

TEST_F(Ha3SeekParaMergeOpTest, testMergeErrors) {
    Ha3SeekParaMergeOp *mergeOp;
    prepareSeekParaMergeOp(2, mergeOp);
    {
        TableInfoPtr tableInfoPtr;
        map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        vector<SearchCommonResourcePtr> commonResources;
        SearchCommonResourcePtr resource1(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        SearchCommonResourcePtr resource2(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        resource1->errorResult->addError(ERROR_UNKNOWN, "ERROR_UNKNOWN");
        resource2->errorResult->addError(ERROR_GENERAL, "ERROR_GENERAL");
        commonResources.push_back(resource1);
        commonResources.push_back(resource2);
        SearchCommonResourcePtr outCommonResource(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        mergeOp->mergeErrors(commonResources, outCommonResource);
        string expect("\nError[1] Msg[ERROR_UNKNOWN]\nError[2] Msg[ERROR_GENERAL]");
        ASSERT_EQ(expect, outCommonResource->errorResult->toDebugString());
    }
    { // ERROR_NONE
        TableInfoPtr tableInfoPtr;
        map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        vector<SearchCommonResourcePtr> commonResources;
        SearchCommonResourcePtr resource1(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        SearchCommonResourcePtr resource2(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        resource1->errorResult->addError(ERROR_NONE, "ERROR_NONE");
        resource2->errorResult->addError(ERROR_NONE, "ERROR_NONE");
        commonResources.push_back(resource1);
        commonResources.push_back(resource2);
        SearchCommonResourcePtr outCommonResource(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        mergeOp->mergeErrors(commonResources, outCommonResource);
        string expect("No Error");
        ASSERT_EQ(expect, outCommonResource->errorResult->toDebugString());
    }
    { //one error
        TableInfoPtr tableInfoPtr;
        map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        vector<SearchCommonResourcePtr> commonResources;
        SearchCommonResourcePtr resource1(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        SearchCommonResourcePtr resource2(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        resource2->errorResult->addError(ERROR_GENERAL, "ERROR_GENERAL");
        commonResources.push_back(resource1);
        commonResources.push_back(resource2);
        SearchCommonResourcePtr outCommonResource(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        mergeOp->mergeErrors(commonResources, outCommonResource);
        string expect("\nError[2] Msg[ERROR_GENERAL]");
        ASSERT_EQ(expect, outCommonResource->errorResult->toDebugString());
    }
    { //none error
        TableInfoPtr tableInfoPtr;
        map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        vector<SearchCommonResourcePtr> commonResources;
        SearchCommonResourcePtr resource1(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        SearchCommonResourcePtr resource2(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        commonResources.push_back(resource1);
        commonResources.push_back(resource2);
        SearchCommonResourcePtr outCommonResource(new SearchCommonResource(&_pool,
                        tableInfoPtr, NULL, NULL, NULL, NULL, CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        mergeOp->mergeErrors(commonResources, outCommonResource);
        string expect("No Error");
        ASSERT_EQ(expect, outCommonResource->errorResult->toDebugString());
    }
}

TEST_F(Ha3SeekParaMergeOpTest, testMergeMertrics) {
    Ha3SeekParaMergeOp *mergeOp;
    prepareSeekParaMergeOp(2, mergeOp);
    { //empty
        TableInfoPtr tableInfoPtr;
        map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        vector<SearchCommonResourcePtr> commonResources;
        SessionMetricsCollectorPtr metricsCollector1(new SessionMetricsCollector());
        SearchCommonResourcePtr resource1(new SearchCommonResource(&_pool,
                        tableInfoPtr, metricsCollector1.get(), NULL, NULL, NULL,
                        CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        SessionMetricsCollectorPtr metricsCollector2(new SessionMetricsCollector());
        SearchCommonResourcePtr resource2(new SearchCommonResource(&_pool,
                        tableInfoPtr, metricsCollector2.get(), NULL, NULL, NULL,
                        CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        commonResources.push_back(resource1);
        commonResources.push_back(resource2);
        kmonitor::MetricsReporter userMetricReporter("", "", {});
        SessionMetricsCollectorPtr outCollector(new SessionMetricsCollector());
        SearchCommonResourcePtr outCommonResource(new SearchCommonResource(&_pool,
                        tableInfoPtr, outCollector.get(), NULL, NULL, NULL,
                        CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));

        mergeOp->mergeMetrics(commonResources, &userMetricReporter,
                              outCommonResource);
        ASSERT_EQ(-1, outCollector->_seekCount);
        ASSERT_EQ(-1, outCollector->_matchCount);
        ASSERT_EQ(-1, outCollector->_strongJoinFilterCount);
        ASSERT_EQ(-1, outCollector->_estimatedMatchCount);
        ASSERT_EQ(-1, outCollector->_aggregateCount);
        ASSERT_EQ(-1, outCollector->_reRankCount);
        ASSERT_EQ(0, outCollector->_seekDocCount);
        ASSERT_EQ(false, outCollector->_useTruncateOptimizer);
    }
    { //one has value
        TableInfoPtr tableInfoPtr;
        map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        vector<SearchCommonResourcePtr> commonResources;
        SessionMetricsCollectorPtr mc1(new SessionMetricsCollector());
        mc1->_seekCount = 10;
        mc1->_matchCount = 11;
        mc1->_strongJoinFilterCount = 12;
        mc1->_estimatedMatchCount = 13;
        mc1->_aggregateCount = 14;
        mc1->_reRankCount = 15;
        mc1->_seekDocCount = 16;
        mc1->_useTruncateOptimizer = true;
        SearchCommonResourcePtr resource1(new SearchCommonResource(&_pool,
                        tableInfoPtr, mc1.get(), NULL, NULL, NULL,
                        CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        SessionMetricsCollectorPtr mc2(new SessionMetricsCollector());
        SearchCommonResourcePtr resource2(new SearchCommonResource(&_pool,
                        tableInfoPtr, mc2.get(), NULL, NULL, NULL,
                        CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        commonResources.push_back(resource1);
        commonResources.push_back(resource2);
        kmonitor::MetricsReporter userMetricReporter("", "", {});
        SessionMetricsCollectorPtr outCollector(new SessionMetricsCollector());
        SearchCommonResourcePtr outCommonResource(new SearchCommonResource(&_pool,
                        tableInfoPtr, outCollector.get(), NULL, NULL, NULL,
                        CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));

        mergeOp->mergeMetrics(commonResources, &userMetricReporter,
                              outCommonResource);
        ASSERT_EQ(10, outCollector->_seekCount);
        ASSERT_EQ(11, outCollector->_matchCount);
        ASSERT_EQ(12, outCollector->_strongJoinFilterCount);
        ASSERT_EQ(13, outCollector->_estimatedMatchCount);
        ASSERT_EQ(14, outCollector->_aggregateCount);
        ASSERT_EQ(15, outCollector->_reRankCount);
        ASSERT_EQ(16, outCollector->_seekDocCount);
        ASSERT_EQ(true, outCollector->_useTruncateOptimizer);
    }
    { //both has value
        TableInfoPtr tableInfoPtr;
        map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        vector<SearchCommonResourcePtr> commonResources;
        SessionMetricsCollectorPtr mc1(new SessionMetricsCollector());
        mc1->_seekCount = 10;
        mc1->_matchCount = 11;
        mc1->_strongJoinFilterCount = 12;
        mc1->_estimatedMatchCount = 13;
        mc1->_aggregateCount = 14;
        mc1->_reRankCount = 15;
        mc1->_seekDocCount = 16;
        mc1->_useTruncateOptimizer = true;
        SearchCommonResourcePtr resource1(new SearchCommonResource(&_pool,
                        tableInfoPtr, mc1.get(), NULL, NULL, NULL,
                        CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));

        SessionMetricsCollectorPtr mc2(new SessionMetricsCollector());
        mc2->_seekCount = 20;
        mc2->_matchCount = 21;
        mc2->_strongJoinFilterCount = 22;
        mc2->_estimatedMatchCount = 23;
        mc2->_aggregateCount = 24;
        mc2->_reRankCount = 25;
        mc2->_seekDocCount = 26;
        mc2->_useTruncateOptimizer = true;
        SearchCommonResourcePtr resource2(new SearchCommonResource(&_pool,
                        tableInfoPtr, mc2.get(), NULL, NULL, NULL,
                        CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));
        commonResources.push_back(resource1);
        commonResources.push_back(resource2);
        kmonitor::MetricsReporter userMetricReporter("", "", {});
        SessionMetricsCollectorPtr outCollector(new SessionMetricsCollector());
        SearchCommonResourcePtr outCommonResource(new SearchCommonResource(&_pool,
                        tableInfoPtr, outCollector.get(), NULL, NULL, NULL,
                        CavaPluginManagerPtr(),
                        NULL, NULL, cavaJitModules));

        mergeOp->mergeMetrics(commonResources, &userMetricReporter,
                              outCommonResource);
        ASSERT_EQ(30, outCollector->_seekCount);
        ASSERT_EQ(32, outCollector->_matchCount);
        ASSERT_EQ(34, outCollector->_strongJoinFilterCount);
        ASSERT_EQ(36, outCollector->_estimatedMatchCount);
        ASSERT_EQ(38, outCollector->_aggregateCount);
        ASSERT_EQ(40, outCollector->_reRankCount);
        ASSERT_EQ(42, outCollector->_seekDocCount);
        ASSERT_EQ(true, outCollector->_useTruncateOptimizer);
    }
}
END_HA3_NAMESPACE();
