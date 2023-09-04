#pragma once

#include <vector>

#include "indexlib/document/kkv/KKVDocumentBatch.h"
#include "indexlib/framework/TabletReader.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/kkv_table/KKVReader.h"
#include "indexlib/table/test/TableTestHelper.h"

namespace indexlibv2::framework {
class Locator;
}

namespace indexlibv2 { namespace table {
class KKVTableTestHelper : public TableTestHelper
{
public:
    KKVTableTestHelper(bool autoCleanIndex = true, bool needDeploy = true) : TableTestHelper(autoCleanIndex, needDeploy)
    {
        _metricsCollector.reset(new index::KVMetricsCollector());
    }
    ~KKVTableTestHelper() = default;

    Status DoBuild(std::string docs, bool oneBatch = false) override;
    Status DoBuild(const std::string& docStr, const framework::Locator& locator) override;

    bool DoQuery(std::string indexType, std::string indexName, std::string queryStr, std::string expectValue) override;

public:
    // for test that only has kkv reader
    bool DoQuery(const std::shared_ptr<indexlibv2::table::KKVReader>& reader,
                 const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig, std::string queryStr,
                 std::string expectValue);
    bool DoQuery(std::string indexType, std::string indexName, std::string queryStr,
                 indexlibv2::table::KKVReadOptions readOptions, std::string expectValue);

    bool DoBatchQuery(const std::string& indexType, const std::string& indexName,
                      const std::vector<std::string>& queryStr, indexlibv2::table::KKVReadOptions& readOptions,
                      const std::string& expectValue, bool batchFinish);

    bool DoQueryWithError(std::string indexType, std::string indexName, std::string queryStr,
                          indexlibv2::table::KKVReadOptions readOptions);

private:
    bool DoQueryWithReadOption(const std::shared_ptr<indexlibv2::table::KKVReader>& reader,
                               const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                               std::string queryStr, indexlibv2::table::KKVReadOptions readOptions, bool expectedError,
                               std::string expectValue);

    bool DoBatchQueryWithReadOption(const std::shared_ptr<indexlibv2::table::KKVReader>& reader,
                                    const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                    const std::vector<std::string>& queryStr,
                                    indexlibv2::table::KKVReadOptions& readOptions, bool expectedError,
                                    const std::string& expectValue, bool batchFinish);

private:
    void PrepareResource(const std::string& indexType, const std::string& indexName,
                         std::shared_ptr<indexlibv2::table::KKVReader>& kkvReader,
                         std::shared_ptr<indexlibv2::config::KKVIndexConfig>& kkvIndexConfig);

    void ParseQuery(const std::string& queryStr, std::string& pkey, std::vector<std::string>& skeys,
                    uint64_t& timestamp);

    std::shared_ptr<index::KVMetricsCollector>& GetMetricsCollector() { return _metricsCollector; };
    void DestructMetricsCollector() { _metricsCollector.reset(); }
    void ConstructMetricsCollector() { _metricsCollector.reset(new index::KVMetricsCollector()); }

    autil::mem_pool::Pool& GetPool() { return _pool; }

private:
    Status DoBuild(const std::string& docStr, bool oneBatch, const framework::Locator* locator);

public:
    static std::shared_ptr<indexlibv2::config::ITabletSchema> LoadSchema(const std::string& dir,
                                                                         const std::string& schemaFileName);
    static std::shared_ptr<indexlibv2::config::ITabletSchema>
    LoadSchemaByJson(const autil::legacy::json::JsonMap& json);
    static autil::legacy::json::JsonMap LoadSchemaJson(const std::string& path);
    static bool UpdateSchemaJsonNode(autil::legacy::json::JsonMap& json, const std::string& jsonPath,
                                     autil::legacy::Any value);

    static std::shared_ptr<indexlibv2::config::KKVIndexConfig>
    GetIndexConfig(std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, const std::string& indexName);
    // for gtest param
    static void SetIndexConfigValueParam(std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                         const std::string& valueFormat);

    static std::shared_ptr<config::TabletOptions> CreateTableOptions(const std::string& jsonStr);

protected:
    std::shared_ptr<TableTestHelper> CreateMergeHelper() override;

private:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<index::KVMetricsCollector> _metricsCollector = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
