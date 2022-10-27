#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/search/JitNormalAggregator.h>
#include <ha3/search/AggregatorCreator.h>
#include <ha3/common/XMLResultFormatter.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <indexlib/partition/index_application.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/function/FunctionInterfaceCreator.h>
#include <suez/turing/common/QueryResource.h>
#include <suez/turing/common/FileUtil.h>
#include <suez/turing/common/CavaJitWrapper.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <suez/turing/expression/syntax/SyntaxExprValidator.h>

using namespace std;
using namespace testing;
using namespace autil;
using namespace suez::turing;
using namespace autil::legacy;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(testlib);
USE_HA3_NAMESPACE(queryparser);

BEGIN_HA3_NAMESPACE(search);

class AggregatorPerfTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    std::string formatAggresult(Aggregator *agg,
                                std::vector<matchdoc::MatchDoc> &docs);
    std::string genDocs();
    void perfTest(const std::string &aggDescStr);
private:
    autil::mem_pool::Pool *_pool = nullptr;
    common::Ha3MatchDocAllocator *_allocator = nullptr;
    tensorflow::QueryResource *_queryResource = nullptr;
    CavaJitWrapperPtr _cavaJitWrapper;
    CavaPluginManagerPtr _cavaPluginManager;
    suez::turing::FunctionInterfaceCreator *_funcCreator = nullptr;
    KeyValueMap _kvpairs;
    std::string _testPath;
    IndexApplication::IndexPartitionMap _indexPartitionMap;
    IndexApplicationPtr _indexApp;
    PartitionReaderSnapshotPtr _snapshotPtr;
    suez::turing::TableInfoPtr _tableInfo;
    suez::turing::AttributeExpressionCreator *_attributeExpressionCreator = nullptr;
    suez::turing::FunctionProvider *_functionProvider = nullptr;
    SyntaxExprValidator *_syntaxExprValidator = nullptr;
    std::vector<matchdoc::MatchDoc> _matchdocs;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, AggregatorPerfTest);

void AggregatorPerfTest::setUp() {
    _testPath = GET_TEMP_DATA_PATH() + "agg_perf_path/";
    if (!_testPath.empty()) {
        FileSystemWrapper::DeleteIfExist(_testPath);
        FileSystemWrapper::DeleteIfExist(_testPath+"mainTable");
    }
    ::cava::CavaJit::globalInit();
    _pool = new autil::mem_pool::Pool(1024);
    _allocator = new common::Ha3MatchDocAllocator(_pool, true);
    _queryResource = new tensorflow::QueryResource();
    suez::turing::CavaConfig cavaConfigInfo;
    cavaConfigInfo._cavaConf = GET_TEST_DATA_PATH() + "jit_agg/ha3_cava_config.json";
    _cavaJitWrapper.reset(new suez::turing::CavaJitWrapper("", cavaConfigInfo, NULL));
    ASSERT_TRUE(_cavaJitWrapper->init());
    _queryResource->setCavaJitWrapper(_cavaJitWrapper.get());
    _queryResource->setCavaAllocator(SuezCavaAllocatorPtr(new SuezCavaAllocator(_pool, 256 * 1024 * 1024)));
    _funcCreator = new FunctionInterfaceCreator();
    FuncConfig fc;
    _funcCreator->init(fc, {});

    // main
    {
        auto schema = IndexlibPartitionCreator::CreateSchema(
                "mainTable",
                "pk:string;key:int32;val:int32", // fields
                "pk:primarykey64:pk", //indexs
                "key;val", //attributes
                "", // summarys
                ""); // truncateProfile
        auto subSchema = IndexlibPartitionCreator::CreateSchema(
            "subTableName",
            "sub_pk:string;sub_price:int32;", // fields
            "sub_pk:primarykey64:sub_pk;", //indexs
            "sub_price", //attributes
            "", // summarys
            ""); // truncateProfile

        string docStrs = genDocs();
        IndexPartitionOptions options;
        auto indexPartition = IndexlibPartitionCreator::CreateIndexPartition(
                IndexlibPartitionCreator::CreateSchemaWithSub(schema, subSchema),
                _testPath + "mainTable", docStrs, options);
        string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        _indexApp.reset(new IndexApplication);
        _indexApp->Init(_indexPartitionMap, JoinRelationMap());
        _snapshotPtr = _indexApp->CreateSnapshot();
        _tableInfo = TableInfoConfigurator::createFromIndexApp("mainTable", _indexApp);
    }

    _functionProvider = new FunctionProvider(_allocator, _pool,
            _queryResource->getCavaAllocator(), nullptr, _snapshotPtr.get(), &_kvpairs);
    _cavaPluginManager.reset(new CavaPluginManager(_cavaJitWrapper, nullptr));
    VirtualAttributes virtualAttributes;
    _attributeExpressionCreator = new AttributeExpressionCreator(_pool,
            _allocator, "mainTable", _snapshotPtr.get(), _tableInfo,
            virtualAttributes, _funcCreator, _cavaPluginManager, _functionProvider);
    _syntaxExprValidator = new SyntaxExprValidator(_tableInfo->getAttributeInfos(),
            virtualAttributes, true);
}

void AggregatorPerfTest::tearDown() {
    DELETE_AND_SET_NULL(_syntaxExprValidator);
    _tableInfo.reset();
    _snapshotPtr.reset();
    _indexPartitionMap.clear();
    _indexApp.reset();
    delete _allocator;
    delete _queryResource;
    delete _funcCreator;
    delete _functionProvider;
    delete _attributeExpressionCreator;
    _cavaPluginManager.reset();
    _cavaJitWrapper.reset();
    DELETE_AND_SET_NULL(_pool);
    if (!_testPath.empty()) {
        FileSystemWrapper::DeleteIfExist(_testPath);
        FileSystemWrapper::DeleteIfExist(_testPath+"mainTable");
    }
}

std::string AggregatorPerfTest::formatAggresult(Aggregator *agg,
        std::vector<matchdoc::MatchDoc> &docs)
{
    double t1 = autil::TimeUtility::currentTime() / 1000.;
    agg->batchAggregate(docs);
    agg->endLayer(1.0);
    double t2 = autil::TimeUtility::currentTime() / 1000.;
    auto aggRes = agg->collectAggregateResult();
    double t3 = autil::TimeUtility::currentTime() / 1000.;
    std::cout << agg->getName() << " use " << t2 - t1 << "ms for batchAggregate" << std::endl;
    std::cout << agg->getName() << " use " << t3 - t2 << "ms for collectAggregateResult" << std::endl;
    ResultPtr result(new Result());
    result->fillAggregateResults(aggRes);
    auto aggResultCount = result->getAggregateResultCount();
    for (uint32_t i = 0; i < aggResultCount; ++i) {
        AggregateResultPtr aggResult = result->getAggregateResult(i);
        if (aggResult) {
            aggResult->constructGroupValueMap();
        }
    }
    XMLResultFormatter formatter;
    std::stringstream ss;
    formatter.format(result, ss);
    auto ret = ss.str();
    return ret;
}

std::string AggregatorPerfTest::genDocs() {
    std::string docs;
    int docCount = 100000;
    docs.reserve(docCount * 1024);
    std::random_device rd;
    std::mt19937_64 mt(rd());
    std::uniform_int_distribution<int> keyDist(0, 10000);
    std::uniform_int_distribution<int> valDist(0, 128);
    for (int i = 0 ; i < docCount; i++) {
        docs += "cmd=add,pk=";
        docs += std::to_string(i);
        docs += ",key=";
        docs += std::to_string(keyDist(mt));
        docs += ",val=";
        docs += std::to_string(valDist(mt));
        docs += ",sub_pk=";
        docs += std::to_string(i + 20);
        docs += ",sub_price=";
        docs += std::to_string(i + 30);
        docs += ";";
    }
    // FileUtil::readFileContent("/ssd/1/sance/source/ha3/docs.data", docs);
    // FileUtil::writeToFile("/ssd/1/sance/source/ha3/docs.data", docs);
    for (int i = 0; i < docCount; i++) {
        _matchdocs.push_back(_allocator->allocate(i));
    }
    return docs;
}

void AggregatorPerfTest::perfTest(const std::string &aggDescStr) {
    std::cout << aggDescStr <<std::endl;
    AggregatorCreator aggCreator(_attributeExpressionCreator, _pool, _queryResource);
    aggCreator._aggSamplerConfigInfo._maxSortCount = 10000;
    AggregateClause aggClause;
    ClauseParserContext ctx;
    ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
    AggregateDescription *aggDescription = ctx.stealAggDescription();
    // validate groupKeyExpr
    auto groupKeyExpr = aggDescription->getGroupKeyExpr();
    _syntaxExprValidator->validate(groupKeyExpr);
    auto errorCode = _syntaxExprValidator->getErrorCode();
    ASSERT_EQ(ERROR_NONE, errorCode);
    // validate func
    auto &aggFunDesVec = aggDescription->getAggFunDescriptions();
    for (size_t i = 0; i < aggFunDesVec.size(); i++) {
        if (aggFunDesVec[i]->getFunctionName() == "count") {
            continue;
        }
        auto syntaxExpr = aggFunDesVec[i]->getSyntaxExpr();
        _syntaxExprValidator->validate(syntaxExpr);
        errorCode = _syntaxExprValidator->getErrorCode();
        ASSERT_EQ(ERROR_NONE, errorCode);
    }
    aggClause.addAggDescription(aggDescription);
    Aggregator *agg = aggCreator.createAggregator(&aggClause);
    ASSERT_TRUE(agg != NULL);
    std::string code;
#define CASE(vt_type)                                                   \
    case vt_type: {                                                     \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;     \
        if (groupKeyExpr->isMultiValue()) {                             \
            typedef NormalAggregator<T, autil::MultiValueType<T>> MAggT; \
            (dynamic_cast<MAggT *>(agg))->constructCavaCode(code);      \
        } else {                                                        \
            typedef NormalAggregator<T> AggT;                           \
            (dynamic_cast<AggT *>(agg))->constructCavaCode(code);       \
        }                                                               \
        break;                                                          \
    }

    switch (groupKeyExpr->getExprResultType()) {
        CASE(vt_int8)
        CASE(vt_int16)
        CASE(vt_int32)
        CASE(vt_int64)
        CASE(vt_uint8)
        CASE(vt_uint16)
        CASE(vt_uint32)
        CASE(vt_uint64)
        CASE(vt_float)
        CASE(vt_double)
        CASE(vt_string)
    default:
        break;
    }
#undef CASE

    // std::cout << code << std::endl;
    size_t hashKey = _cavaJitWrapper->calcHashKey({code});
    double ts = autil::TimeUtility::currentTime() / 1000.;
    cava::CavaModuleOptions options;
    options.safeCheck = cava::SafeCheckLevel::SCL_NONE;
    options.printModule = false;
    auto cavaJitModule = _cavaJitWrapper->compile("test", {"test"}, {code}, hashKey, &options);
    double te = autil::TimeUtility::currentTime() / 1000.;
    std::cout << "compile use " << te - ts << "ms" << std::endl;
    ASSERT_TRUE(cavaJitModule != NULL);
    auto cavaAggModuleInfo = agg->codegen();
    ASSERT_TRUE(cavaAggModuleInfo != NULL);

#define CASE(vt_type)                                                   \
    case vt_type: {                                                     \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;     \
        if (groupKeyExpr->isMultiValue()) {                             \
            typedef NormalAggregator<T, autil::MultiValueType<T>> MAggT; \
            auto aggTyped = dynamic_cast<MAggT *>(agg);                 \
            typedef JitNormalAggregator<T, autil::MultiValueType<T>> JitMAggT; \
            jitAgg = new JitMAggT(aggTyped, _pool,                      \
                    cavaAggModuleInfo, _queryResource->getCavaAllocator()); \
        } else {                                                        \
            typedef NormalAggregator<T> AggT;                           \
            auto aggTyped = dynamic_cast<AggT *>(agg);                  \
            typedef JitNormalAggregator<T> JitAggT;                     \
            jitAgg = new JitAggT(aggTyped, _pool,                       \
                    cavaAggModuleInfo, _queryResource->getCavaAllocator()); \
        }                                                               \
        break;                                                          \
    }

    Aggregator *jitAgg = NULL;
    switch (groupKeyExpr->getExprResultType()) {
        CASE(vt_int8)
        CASE(vt_int16)
        CASE(vt_int32)
        CASE(vt_int64)
        CASE(vt_uint8)
        CASE(vt_uint16)
        CASE(vt_uint32)
        CASE(vt_uint64)
        CASE(vt_float)
        CASE(vt_double)
        CASE(vt_string)
    default:
        break;
    }
#undef CASE

    if (jitAgg) {
        auto aggResStr = formatAggresult(agg, _matchdocs);
        auto jitAggResStr = formatAggresult(jitAgg, _matchdocs);
        // std::cout << aggResStr << std::endl;
        // auto aggResStr = formatAggresult(agg, _matchdocs);
        // std::cout << jitAggResStr << std::endl;
        DELETE_AND_SET_NULL(jitAgg);
        ASSERT_TRUE(aggResStr == jitAggResStr);
    } else {
        DELETE_AND_SET_NULL(agg);
    }
}

TEST_F(AggregatorPerfTest, testSimple) {
    perfTest("group_key:key,agg_fun:count()");
    perfTest("group_key:key,agg_fun:count()#sum(sub_price)");
    perfTest("group_key:key,agg_fun:sum(val),max_group:20000");
    perfTest("group_key:key,agg_fun:min(val),max_group:20000");
    perfTest("group_key:key,agg_fun:max(val),max_group:20000");
    perfTest("group_key:key,agg_fun:min(val)#count()");
    perfTest("group_key:key,agg_fun:min(val)#max(val)#count()");
    perfTest("group_key:key,agg_fun:min(val)#max(val)#sum(val)#count()");
}

END_HA3_NAMESPACE();
