#include "indexlib/table/kkv_table/test/KKVTableTestHelper.h"

#include "autil/StringUtil.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/kkv_table/KKVSchemaResolver.h"
#include "indexlib/table/kkv_table/test/KKVTableTestSearcher.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "indexlib/table/test/DocumentParser.h"
#include "indexlib/table/test/ResultChecker.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlibv2::config;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KKVTableTestHelper);

std::shared_ptr<TableTestHelper> KKVTableTestHelper::CreateMergeHelper()
{
    std::shared_ptr<TableTestHelper> ptr(new KKVTableTestHelper);
    return ptr;
}

void KKVTableTestHelper::PrepareResource(const std::string& indexType, const std::string& indexName,
                                         std::shared_ptr<indexlibv2::table::KKVReader>& kkvReader,
                                         std::shared_ptr<indexlibv2::config::KKVIndexConfig>& kkvIndexConfig)
{
    assert(_schema->GetTableType() == "kkv");
    auto tabletReader = GetTabletReader();
    assert(tabletReader);
    auto indexReader = tabletReader->GetIndexReader(indexType, indexName);
    assert(indexReader);
    kkvReader = std::dynamic_pointer_cast<indexlibv2::table::KKVReader>(indexReader);
    assert(kkvReader);
    const auto& indexConfig = _schema->GetIndexConfig(indexType, indexName);
    assert(indexConfig);
    kkvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfig);
    assert(kkvIndexConfig);
}

bool KKVTableTestHelper::DoQuery(std::string indexType, std::string indexName, std::string queryStr,
                                 std::string expectValue)
{
    std::shared_ptr<indexlibv2::table::KKVReader> kkvReader;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> kkvIndexConfig;
    PrepareResource(indexType, indexName, kkvReader, kkvIndexConfig);

    indexlibv2::table::KKVReadOptions readOptions;
    return DoQueryWithReadOption(kkvReader, kkvIndexConfig, queryStr, readOptions, false, expectValue);
}

bool KKVTableTestHelper::DoQuery(std::string indexType, std::string indexName, std::string queryStr,
                                 indexlibv2::table::KKVReadOptions readOptions, std::string expectValue)
{
    std::shared_ptr<indexlibv2::table::KKVReader> kkvReader;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> kkvIndexConfig;
    PrepareResource(indexType, indexName, kkvReader, kkvIndexConfig);

    return DoQueryWithReadOption(kkvReader, kkvIndexConfig, queryStr, readOptions, false, expectValue);
}

bool KKVTableTestHelper::DoQueryWithError(std::string indexType, std::string indexName, std::string queryStr,
                                          indexlibv2::table::KKVReadOptions readOptions)
{
    std::shared_ptr<indexlibv2::table::KKVReader> kkvReader;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> kkvIndexConfig;
    PrepareResource(indexType, indexName, kkvReader, kkvIndexConfig);

    return DoQueryWithReadOption(kkvReader, kkvIndexConfig, queryStr, readOptions, true, "");
}

bool KKVTableTestHelper::DoQuery(const std::shared_ptr<indexlibv2::table::KKVReader>& reader,
                                 const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                 std::string queryStr, std::string expectValue)
{
    indexlibv2::table::KKVReadOptions readOptions;
    return DoQueryWithReadOption(reader, indexConfig, queryStr, readOptions, false, expectValue);
}

void KKVTableTestHelper::ParseQuery(const std::string& queryStr, std::string& pkey, std::vector<std::string>& skeys,
                                    uint64_t& timestamp)
{
    std::vector<std::string> queryInfos = StringUtil::split(queryStr, "#");
    if (queryInfos.size() > 2) {
        assert(false);
    } else if (queryInfos.size() == 2) {
        timestamp = StringUtil::fromString<uint64_t>(queryInfos[1]);
    }

    vector<string> keyInfos = StringUtil::split(queryInfos[0], ":");
    pkey = keyInfos[0];

    if (keyInfos.size() == 2) {
        skeys = StringUtil::split(keyInfos[1], ",");
    }
}

bool KKVTableTestHelper::DoQueryWithReadOption(const std::shared_ptr<indexlibv2::table::KKVReader>& reader,
                                               const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                               std::string queryStr, indexlibv2::table::KKVReadOptions readOptions,
                                               bool expectedError, std::string expectValue)
{
    KKVTableTestSearcher searcher;
    searcher.Init(reader, indexConfig);

    std::string pkey;
    std::vector<std::string> skeys;
    uint64_t timestamp = 0;
    ParseQuery(queryStr, pkey, skeys, timestamp);

    if (_metricsCollector) {
        _metricsCollector->Reset();
        readOptions.metricsCollector = _metricsCollector.get();
    }

    readOptions.pool = &_pool;
    readOptions.timestamp = timestamp;
    auto result = searcher.Search(pkey, skeys, readOptions);
    auto expectResult = DocumentParser::ParseResult(expectValue);

    return ResultChecker::Check(result, expectedError, expectResult);
}

bool KKVTableTestHelper::DoBatchQuery(const std::string& indexType, const std::string& indexName,
                                      const vector<std::string>& queryStr,
                                      indexlibv2::table::KKVReadOptions& readOptions, const std::string& expectValue,
                                      bool batchFinish)
{
    std::shared_ptr<indexlibv2::table::KKVReader> kkvReader;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> kkvIndexConfig;
    PrepareResource(indexType, indexName, kkvReader, kkvIndexConfig);

    return DoBatchQueryWithReadOption(kkvReader, kkvIndexConfig, queryStr, readOptions, false, expectValue,
                                      batchFinish);
}

bool KKVTableTestHelper::DoBatchQueryWithReadOption(
    const std::shared_ptr<indexlibv2::table::KKVReader>& reader,
    const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig, const vector<std::string>& queryStrVec,
    indexlibv2::table::KKVReadOptions& readOptions, bool expectedError, const std::string& expectValue,
    bool batchFinish)
{
    KKVTableTestSearcher searcher;
    searcher.Init(reader, indexConfig);

    vector<string> pkeyStrVec;
    vector<vector<string>> skeysVec;
    // all timestamp in each query should be same
    uint64_t timestamp = 0;
    for (auto& queryStr : queryStrVec) {
        std::string pkey;
        std::vector<std::string> skeys;
        ParseQuery(queryStr, pkey, skeys, timestamp);

        pkeyStrVec.push_back(pkey);
        skeysVec.push_back(skeys);
    }

    if (_metricsCollector) {
        _metricsCollector->Reset();
        readOptions.metricsCollector = _metricsCollector.get();
    }

    readOptions.pool = &_pool;
    readOptions.timestamp = timestamp;
    auto result = searcher.Search(pkeyStrVec, skeysVec, readOptions, batchFinish);
    auto expectResult = DocumentParser::ParseResult(expectValue);

    return ResultChecker::Check(result, expectedError, expectResult);
}

Status KKVTableTestHelper::DoBuild(std::string docs, bool oneBatch) { return DoBuild(docs, oneBatch, nullptr); }

Status KKVTableTestHelper::DoBuild(const std::string& docStr, const framework::Locator& locator)
{
    return DoBuild(docStr, false, &locator);
}

Status KKVTableTestHelper::DoBuild(const std::string& docStr, bool oneBatch, const framework::Locator* locator)
{
    auto docBatch = document::KVDocumentBatchMaker::Make(_schema, docStr);
    if (!docBatch) {
        return Status::InternalError("make document batch from %s failed", docStr.c_str());
    }
    if (oneBatch) {
        auto status = GetITablet()->Build(docBatch);
        if (status.IsUninitialize()) {
            status = GetITablet()->Build(docBatch);
        }
        return status;
    } else {
        auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch.get());
        while (iter->HasNext()) {
            auto kvDoc = std::dynamic_pointer_cast<document::KKVDocument>(iter->Next());
            if (locator != nullptr) {
                kvDoc->SetLocator(*locator);
            }
            auto singleDocBatch = std::make_shared<document::KVDocumentBatch>();
            singleDocBatch->AddDocument(kvDoc);
            auto status = GetITablet()->Build(singleDocBatch);
            if (status.IsUninitialize()) {
                status = GetITablet()->Build(singleDocBatch);
            }
            if (!status.IsOK()) {
                return status;
            }
        }
        return Status::OK();
    }
}

bool KKVTableTestHelper::UpdateSchemaJsonNode(JsonMap& json, const std::string& jsonPath, Any value)
{
    std::vector<std::string> nodePathVec = autil::StringUtil::split(jsonPath, ".");
    assert(nodePathVec.size() != 0);
    JsonMap* node = &json;
    for (size_t i = 0; i < nodePathVec.size() - 1; ++i) {
        std::string& nodePath = nodePathVec[i];
        bool isJsonArray = false;
        int arrayIndex = 0;
        auto arrayStartPos = nodePath.find("[");
        if (arrayStartPos != std::string::npos) {
            isJsonArray = true;
            auto arrayIndexLen = nodePath.size() - arrayStartPos - 2;
            arrayIndex = std::atoi(nodePath.substr(arrayStartPos + 1, arrayIndexLen).c_str());
            nodePath = nodePath.substr(0, arrayStartPos);
        }
        JsonMap::iterator it = node->find(nodePath);
        if (it == node->end()) {
            AUTIL_LOG(ERROR, "no found node: %s, invalid json path: %s", nodePath.c_str(), jsonPath.c_str());
            return false;
        }
        if (isJsonArray) {
            auto jsonArray = AnyCast<JsonArray>(&it->second);
            if (arrayIndex >= jsonArray->size()) {
                AUTIL_LOG(ERROR, "arrayIndex invalid:%d, invalid json path: %s", arrayIndex, jsonPath.c_str());
                return false;
            }
            node = AnyCast<JsonMap>(&((*jsonArray)[arrayIndex]));
        } else {
            node = AnyCast<JsonMap>(&it->second);
        }
        if (node == nullptr) {
            AUTIL_LOG(ERROR, "invalid json path: %s", jsonPath.c_str());
            return false;
        }
    }
    JsonMap& parentNode = *node;
    parentNode[nodePathVec.back()] = value;
    return true;
}

std::shared_ptr<indexlibv2::config::ITabletSchema> KKVTableTestHelper::LoadSchema(const std::string& dir,
                                                                                  const std::string& schemaFileName)
{
    return framework::TabletSchemaLoader::LoadSchema(dir, schemaFileName);
}

autil::legacy::json::JsonMap KKVTableTestHelper::LoadSchemaJson(const std::string& path)
{
    std::string jsonStr;
    indexlib::file_system::ErrorCode ec = indexlib::file_system::FslibWrapper::AtomicLoad(path, jsonStr).Code();
    if (ec == indexlib::file_system::FSEC_NOENT) {
        AUTIL_LOG(ERROR, "load file [%s] failed", path.c_str());
    } else {
        THROW_IF_FS_ERROR(ec, "load file [%s] failed", path.c_str());
    }
    autil::legacy::Any any;
    autil::legacy::json::ParseJson(jsonStr, any);
    auto jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(any);
    return jsonMap;
}

std::shared_ptr<indexlibv2::config::ITabletSchema> KKVTableTestHelper::LoadSchemaByJson(const JsonMap& json)
{
    try {
        std::string jsonStr = autil::legacy::json::ToString(json);
        return framework::TabletSchemaLoader::LoadSchema(jsonStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "tablet schema load failed, exception[%s]", e.what());
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "tablet schema load failed, exception[%s]", e.what());
    } catch (...) {
        AUTIL_LOG(ERROR, "tablet schema load failed, unknown exception");
    }
    return nullptr;
}

std::shared_ptr<indexlibv2::config::KKVIndexConfig>
KKVTableTestHelper::GetIndexConfig(std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                   const std::string& indexName)
{
    auto indexConfig = schema->GetIndexConfig("kkv", indexName);
    assert(indexConfig);
    auto kkvIndexConfig = dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
    assert(kkvIndexConfig);
    return kkvIndexConfig;
}

void KKVTableTestHelper::SetIndexConfigValueParam(std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                                  const std::string& valueFormat)
{
    auto valueConfig = indexConfig->GetValueConfig();
    if (valueFormat == "plain") {
        valueConfig->EnablePlainFormat(true);
    } else if (valueFormat == "impact") {
        valueConfig->EnableValueImpact(true);
    } else {
        valueConfig->EnablePlainFormat(false);
        valueConfig->EnableValueImpact(false);
    }
    indexConfig->SetValueConfig(valueConfig);
}

std::shared_ptr<config::TabletOptions> KKVTableTestHelper::CreateTableOptions(const std::string& jsonStr)
{
    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    return tabletOptions;
}

} // namespace indexlibv2::table
