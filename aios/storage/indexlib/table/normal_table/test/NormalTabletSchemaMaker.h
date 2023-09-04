#pragma once

#include <functional>

#include "autil/Log.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/common/Types.h"

namespace indexlibv2::config {
class SingleFieldIndexConfig;
class FieldConfig;
class InvertedIndexConfig;
class PackageIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::table {

class NormalTabletSchemaMaker
{
public:
    // fieldNames format:
    //     fieldName:fieldType:isMultiValue:FixedMultiValueCount:EnableNullField:AnalyzerName
    // indexNames format:
    //     indexName:indexType:fieldName:hasTruncate:shardingCount:useHashDict
    // attributeNames format:
    //     fieldName:updatable:compressStr
    //     packName:updatable:compressStr:subAttr1|compressStr,subAttr2|compressStr
    // summaryNames format:
    //     fieldName
    // truncateProfiles format:
    //     truncateName:sortDescription
    static std::shared_ptr<config::TabletSchema> Make(const std::string& fieldNames, const std::string& indexNames,
                                                      const std::string& attributeNames,
                                                      const std::string& summaryNames,
                                                      const std::string& truncateProfiles = "");

private:
    static Status MakeFieldConfig(config::UnresolvedSchema* schema, const std::string& fieldNames);
    static Status MakeIndexConfig(config::UnresolvedSchema* schema, const std::string& indexNames);
    static Status MakeAttributeConfig(config::UnresolvedSchema* schema, const std::vector<std::string>& attributeNames);
    static Status MakeSummaryConfig(config::UnresolvedSchema* schema, const std::vector<std::string>& fieldNames);
    static Status MakeTruncateProfiles(config::UnresolvedSchema* schema, const std::string& truncateProfileStr);

    static std::shared_ptr<config::SingleFieldIndexConfig>
    CreateSingleFieldIndexConfig(const std::string& indexName, InvertedIndexType indexType,
                                 const std::string& fieldName, const std::string& analyzerName, bool isVirtual,
                                 const std::shared_ptr<config::FieldConfig>& fieldConfig);
    static void CreateShardingIndexConfigs(const std::shared_ptr<config::InvertedIndexConfig>& indexConfig,
                                           size_t shardingCount);
    static std::shared_ptr<config::PackageIndexConfig>
    CreatePackageIndexConfig(const std::string& indexName, InvertedIndexType indexType,
                             const std::vector<std::string>& fieldNames, const std::vector<int32_t> boosts,
                             const std::string& analyzerName, bool isVirtual, const config::UnresolvedSchema* schema);
    static void SetNeedStoreSummary(config::UnresolvedSchema* schema);
    static Status EnsureSpatialIndexWithAttribute(config::UnresolvedSchema* schema);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
