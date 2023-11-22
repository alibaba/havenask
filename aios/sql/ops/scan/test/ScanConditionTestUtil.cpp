#include "sql/ops/scan/test/ScanConditionTestUtil.h"

#include <assert.h>
#include <cstddef>
#include <limits>
#include <memory>
#include <stdint.h>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/analyzer/AnalyzerInfos.h"
#include "build_service/analyzer/TokenizerManager.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/ResourceReader.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/search/DefaultLayerMetaUtil.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "ha3/turing/common/ModelConfig.h"
#include "indexlib/analyzer/AnalyzerDefine.h"
#include "indexlib/analyzer/AnalyzerInfo.h"
#include "indexlib/analyzer/IAnalyzerFactory.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/online_config.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "sql/common/IndexInfo.h"
#include "sql/ops/scan/ScanIteratorCreatorR.h"
#include "sql/ops/scan/test/FakeTokenizer.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/TableInfoConfigurator.h"

namespace suez {
namespace turing {
class VirtualAttribute;
} // namespace turing
} // namespace suez

using namespace suez::turing;
using namespace build_service::analyzer;
using namespace build_service::config;
using namespace std;
using namespace matchdoc;
using namespace suez::turing;

using namespace isearch::search;
using namespace isearch::common;
using namespace isearch::turing;
using namespace indexlibv2::analyzer;

namespace sql {

void ScanConditionTestUtil::init(const std::string &path, const std::string &testDataPath) {
    _testPath = path + "sql_op_index_path/";
    _tableName = "invertedTable";
    initIndexPartition();
    indexlib::partition::JoinRelationMap joinMap;
    _indexApp.reset(new indexlib::partition::IndexApplication);
    _indexApp->Init(_indexPartitionMap, joinMap);
    _analyzerFactory = initAnalyzerFactory(testDataPath);
    _tableInfo = suez::turing::TableInfoConfigurator::createFromIndexApp(_tableName, _indexApp);
    bool useSubFlag = _tableInfo->hasSubSchema();
    _matchDocAllocator.reset(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
    _indexAppSnapshot = _indexApp->CreateSnapshot();
    std::vector<suez::turing::VirtualAttribute *> virtualAttributes;
    _attributeExpressionCreator.reset(new AttributeExpressionCreator(&_pool,
                                                                     _matchDocAllocator.get(),
                                                                     _tableName,
                                                                     _indexAppSnapshot.get(),
                                                                     _tableInfo,
                                                                     virtualAttributes,
                                                                     NULL,
                                                                     nullptr,
                                                                     NULL));
    _queryInfo.setDefaultIndexName("index_2");
    initParam();
}

AnalyzerFactoryPtr ScanConditionTestUtil::initAnalyzerFactory(const std::string &testDataPath) {
    const std::string fakeAnalyzerName = "fake_analyzer";
    AnalyzerInfosPtr infosPtr(new AnalyzerInfos());
    unique_ptr<AnalyzerInfo> taobaoInfo(new AnalyzerInfo());
    taobaoInfo->setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
    taobaoInfo->setTokenizerConfig(DELIMITER, " ");
    taobaoInfo->addStopWord(string("stop"));
    infosPtr->addAnalyzerInfo("taobao_analyzer", *taobaoInfo);

    string aliwsConfig = testDataPath + string("/default_aliws_conf");
    infosPtr->makeCompatibleWithOldConfig();
    TokenizerManagerPtr tokenizerManagerPtr(
        new TokenizerManager(ResourceReaderPtr(new ResourceReader(aliwsConfig))));
    if (!tokenizerManagerPtr->init(infosPtr->getTokenizerConfig())) {
        return nullptr;
    }

    unique_ptr<AnalyzerInfo> fakeAnalyzerInfo(new AnalyzerInfo());
    fakeAnalyzerInfo->setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
    fakeAnalyzerInfo->setTokenizerConfig(DELIMITER, " ");
    fakeAnalyzerInfo->addStopWord(string("stop"));
    fakeAnalyzerInfo->setTokenizerName(fakeAnalyzerName);
    infosPtr->addAnalyzerInfo(fakeAnalyzerName, *fakeAnalyzerInfo);
    if (2 != infosPtr->getAnalyzerCount()) {
        return nullptr;
    }

    tokenizerManagerPtr->addTokenizer(fakeAnalyzerName, new FakeTokenizer());
    auto *tokenizer = tokenizerManagerPtr->getTokenizerByName(fakeAnalyzerName);
    if (nullptr == tokenizer) {
        return nullptr;
    }
    DELETE_AND_SET_NULL(tokenizer);

    AnalyzerFactory *analyzerFactory = new AnalyzerFactory();
    if (!analyzerFactory->init(infosPtr, tokenizerManagerPtr)) {
        return nullptr;
    }
    AnalyzerFactoryPtr ptr(analyzerFactory);
    if (!ptr->hasAnalyzer(fakeAnalyzerName)) {
        return nullptr;
    }
    if (ptr->hasAnalyzer(fakeAnalyzerName + "1111")) {
        return nullptr;
    }
    return ptr;
}

void ScanConditionTestUtil::initParam() {
    _indexPartitionReaderWrapper = getIndexPartitionReaderWrapper();
    isearch::search::LayerMeta fullLayerMeta
        = isearch::search::DefaultLayerMetaUtil::createFullRange(
            &_pool, _indexPartitionReaderWrapper.get());
    fullLayerMeta.quota = std::numeric_limits<uint32_t>::max();
    _indexInfoMap["name"] = IndexInfo("name", "string");
    _indexInfoMap["index_2"] = IndexInfo("index_2", "string");
    _indexInfoMap["attr2"] = IndexInfo("attr2", "number");

    _param.attrExprCreator = _attributeExpressionCreator;
    _param.indexPartitionReaderWrapper = _indexPartitionReaderWrapper;
    _param.mainTableName = _tableName;
    _param.layerMeta = &fullLayerMeta;
    _param.pool = &_pool;
    _param.analyzerFactory = _analyzerFactory.get();
    _param.indexInfos = _tableInfo->getIndexInfos();
    _param.indexInfo = &_indexInfoMap;
    _param.queryInfo = &_queryInfo;
    _udfToQueryManager.init();
    _param.udfToQueryManager = &_udfToQueryManager;
}

IndexPartitionReaderWrapperPtr ScanConditionTestUtil::getIndexPartitionReaderWrapper() {
    auto indexReaderWrapper = IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
        _indexAppSnapshot.get(), _tableName);
    if (indexReaderWrapper == NULL) {
        return IndexPartitionReaderWrapperPtr();
    }
    indexReaderWrapper->setSessionPool(&_pool);
    return indexReaderWrapper;
}

void ScanConditionTestUtil::initIndexPartition() {
    int64_t ttl;
    string tableName = _tableName;

    auto mainSchema = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
        tableName,
        "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:text",             // fields
        "id:primarykey64:id;index_2:text:index2;name:string:name;attr2:number:attr2", // indexs
        "attr1;attr2;id",                                                             // attributes
        "name",                                                                       // summarys
        ""); // truncateProfile
    auto subSchema = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
        tableName,
        "sub_id:int64",               // fields
        "sub_id:primarykey64:sub_id", // indexs
        "sub_id",                     // attributes
        "",                           // summarys
        "");                          // truncateProfile
    string docsStr = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a a a,sub_id=1^2;"
                     "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a b c,sub_id=3;"
                     "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a c d stop,sub_id=4;"
                     "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b c d,sub_id=5";
    ttl = numeric_limits<int64_t>::max();
    string testPath = _testPath + tableName;
    indexlib::config::IndexPartitionSchemaPtr schema
        = indexlib::testlib::IndexlibPartitionCreator::CreateSchemaWithSub(mainSchema, subSchema);
    assert(schema.get() != nullptr);
    indexlib::config::IndexPartitionOptions options;
    options.GetOnlineConfig().ttl = ttl;
    autil::EnvUtil::setEnv("TEST_QUICK_EXIT", "true", 1);

    indexlib::partition::IndexPartitionPtr indexPartition
        = indexlib::testlib::IndexlibPartitionCreator::CreateIndexPartition(
            schema, testPath, docsStr, options, "", true);
    assert(indexPartition.get() != nullptr);
    string schemaName = indexPartition->GetSchema()->GetSchemaName();
    _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
}

std::shared_ptr<UdfToQueryParam> ScanConditionTestUtil::createUdfToQueryParam() const {
    auto condParam = std::make_shared<UdfToQueryParam>();
    condParam->analyzerFactory = _param.analyzerFactory;
    condParam->indexInfo = _param.indexInfo;
    condParam->indexInfos = _param.indexInfos;
    condParam->queryInfo = _param.queryInfo;
    condParam->modelConfigMap = _param.modelConfigMap;
    condParam->forbidIndexs = _param.forbidIndexs;
    return condParam;
}

} // namespace sql
