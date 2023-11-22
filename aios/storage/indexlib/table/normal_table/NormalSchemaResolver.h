/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/SchemaResolver.h"

namespace indexlib::file_system {
class IDirectory;
}
namespace indexlib::config {
struct SortDescription;
class SummarySchema;
class IndexConfig;
class SortParam;
} // namespace indexlib::config
namespace indexlibv2::config {
class IIndexConfig;
class TabletOptions;
class UnresolvedSchema;
class SummaryIndexConfig;
class InvertedIndexConfig;
class TruncateIndexNameMapper;
class TruncateProfileConfig;
} // namespace indexlibv2::config

namespace indexlibv2::table {

class NormalSchemaResolver : public config::SchemaResolver
{
public:
    NormalSchemaResolver(config::TabletOptions* options) : _options(options) {}
    ~NormalSchemaResolver() = default;

private:
    Status Check(const config::TabletSchema& schema) const override;
    Status LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                      config::UnresolvedSchema* schema) override;
    Status ResolveLegacySchema(const std::string& indexPath, indexlib::config::IndexPartitionSchema* schema) override;
    Status ResolveTabletSchema(const std::string& indexPath, config::UnresolvedSchema* schema) override;

private:
    void DisableTFBitmap(config::UnresolvedSchema* schema);
    Status CheckSortDescs(const config::UnresolvedSchema* schema);
    static Status AddBuiltinIndex(config::UnresolvedSchema* schema);
    Status LoadAdaptiveVocabulary(config::UnresolvedSchema* schema, const std::string& indexPath);
    Status LoadAdaptiveVocabulary(indexlib::config::IndexPartitionSchema* schema, const std::string& indexPath);
    Status ResolveSortDescriptions(config::UnresolvedSchema* schema, const config::TabletOptions* options,
                                   const std::string& indexPath);
    Status FillFileCompressToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                                      config::UnresolvedSchema* schema);
    void FillTTLToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                           config::UnresolvedSchema* schema);
    Status ResolveRuntimeSettings(const std::string& indexPath, config::UnresolvedSchema* schema);
    Status ResolveInvertedIndex(config::UnresolvedSchema* schema);
    Status ResolveAttribute(config::UnresolvedSchema* schema, config::TabletOptions* options);
    Status ResolveTTLField(config::UnresolvedSchema* schema);
    Status ResolveSpatialIndex(config::UnresolvedSchema* schema);
    Status ResolveSummary(config::UnresolvedSchema* schema, config::TabletOptions* options);
    Status ResolveSource(config::UnresolvedSchema* schema, config::TabletOptions* options);

    std::pair<Status, std::shared_ptr<config::InvertedIndexConfig>>
    CreateInvertedIndexConfig(const std::shared_ptr<indexlib::config::IndexConfig>& indexConfig);
    Status FillIndexConfigs(const indexlib::config::IndexPartitionSchema& legacySchema,
                            config::UnresolvedSchema* schema);
    Status ResolveGeneralValue(config::UnresolvedSchema* schema, config::TabletOptions* options);
    Status ResolveTruncateIndexConfigs(const std::string& indexPath, config::UnresolvedSchema* schema);
    Status FillTruncateProfile(const indexlib::config::IndexPartitionSchema& legacySchema,
                               config::UnresolvedSchema* schema) const;
    Status FillDictionaryToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                                    config::UnresolvedSchema* schema) const;
    Status FillAdaptiveDictionaryToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                                            config::UnresolvedSchema* schema) const;
    Status LoadTruncateTermVocabulary(const std::string& indexPath, config::UnresolvedSchema* schema) const;
    Status AppendTruncateIndexConfig(const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
                                     const std::vector<config::TruncateProfileConfig>& truncateProfileConfigs);
    std::shared_ptr<config::InvertedIndexConfig>
    CreateTruncateIndexConfig(const std::shared_ptr<config::InvertedIndexConfig>& invertedIndexConfig,
                              const config::TruncateProfileConfig& truncateProfileConfig) const;
    Status UpdateIndexConfigForTruncate(const std::shared_ptr<config::InvertedIndexConfig>& indexConfig,
                                        const std::shared_ptr<config::InvertedIndexConfig>& truncateIndexConfig);
    Status LoadAdaptiveBitmapTerms(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                   config::UnresolvedSchema* schema);
    Status ResolvePKIndexConfig(config::UnresolvedSchema* schema, config::TabletOptions* options);
    Status ResolveGeneralInvertedIndex(config::UnresolvedSchema* schema, config::TabletOptions* options);
    Status ResolveFieldMeta(config::UnresolvedSchema* schema);
    Status ResolveInvertedIndexId(config::UnresolvedSchema* schema);
    Status CheckStatisticsTermConfig(const config::TabletSchema& schema) const;
    Status
    CheckIndexTruncateProfiles(const std::vector<indexlibv2::config::TruncateProfileConfig>& truncateProfileConfigs,
                               const config::TabletSchema& schema,
                               const std::shared_ptr<config::InvertedIndexConfig>& indexConfig) const;
    Status CheckInvertedIndexConfig(const config::TabletSchema& schema) const;
    Status CheckSummaryConfig(const config::TabletSchema& schema) const;
    Status CheckAttributeConfig(const config::TabletSchema& schema) const;
    Status CheckSpatialIndexConfig(const std::shared_ptr<config::InvertedIndexConfig>& spatialIndexConfig,
                                   const config::TabletSchema& schema) const;
    Status CheckFieldMetaConfig(const config::TabletSchema& schema) const;
    Status
    CheckSingleFieldIndex(const std::shared_ptr<config::InvertedIndexConfig>& invertedIndexConfig,
                          const config::TabletSchema& schema, std::map<fieldid_t, std::string>* fieldId2IndexName,
                          std::map<std::string, std::set<std::string>>* singleFieldIndexConfigsWithProfileNames) const;
    Status CheckTruncateProfileSchema(
        const std::vector<indexlibv2::config::TruncateProfileConfig>& truncateProfileConfigs) const;
    Status CheckTruncateSortParams(const std::vector<indexlibv2::config::TruncateProfileConfig>& truncateProfileConfigs,
                                   const config::TabletSchema& schema) const;
    Status CheckFieldsOrderInPackIndex(const std::shared_ptr<config::InvertedIndexConfig>& invertedIndexConfig,
                                       const config::TabletSchema& schema) const;
    bool IsValidTruncateSortParam(const std::vector<indexlib::config::SortParam>& sortParams,
                                  const config::UnresolvedSchema* schema) const;
    Status
    doLoadAdaptiveBitmap(const std::shared_ptr<indexlib::file_system::IDirectory>& adaptiveBitmapDir,
                         const std::vector<std::shared_ptr<config::InvertedIndexConfig>>& adaptiveDictIndexConfigs);
    void ResolveEmptyProfileNamesForTruncateIndex(
        const std::vector<indexlibv2::config::TruncateProfileConfig>& profileConfigs,
        const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs);

private:
    config::TabletOptions* _options = nullptr;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
