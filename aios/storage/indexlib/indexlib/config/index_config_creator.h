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
#ifndef __INDEXLIB_INDEX_CONFIG_CREATOR_H
#define __INDEXLIB_INDEX_CONFIG_CREATOR_H

#include <memory>

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "indexlib/common_define.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/adaptive_dictionary_schema.h"
#include "indexlib/config/dictionary_schema.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class IndexConfigCreator
{
public:
    IndexConfigCreator();
    ~IndexConfigCreator();

public:
    // to do replace any with string
    // when loadFromIndex = true, format_version will set to 0 when no exist for read compitable usage
    // when load from config, format_version will set to DEFAULT_FORMAT_VERSION when no exist
    // ** when format_version always exist in index schema file, isLoadFromIndex is useless, remove it
    static IndexConfigPtr Create(const FieldSchemaPtr& fieldSchema, const DictionarySchemaPtr& dictSchema,
                                 const AdaptiveDictionarySchemaPtr& adaptDictSchema,
                                 const std::shared_ptr<FileCompressSchema>& fileCompressSchema,
                                 const autil::legacy::Any& indexConfigJson, TableType tableType, bool isLoadFromIndex);

    static SingleFieldIndexConfigPtr CreateSingleFieldIndexConfig(const std::string& indexName,
                                                                  InvertedIndexType indexType,
                                                                  const std::string& fieldName,
                                                                  const std::string& analyzerName, bool isVirtual,
                                                                  const FieldSchemaPtr& fieldSchema);

public:
    // public for test
    static PackageIndexConfigPtr CreatePackageIndexConfig(const std::string& indexName, InvertedIndexType indexType,
                                                          const std::vector<std::string>& fieldNames,
                                                          const std::vector<int32_t> boosts,
                                                          const std::string& analyzerName, bool isVirtual,
                                                          const FieldSchemaPtr& fieldSchema);

    static void CreateShardingIndexConfigs(const config::IndexConfigPtr& indexConfig, size_t shardingCount);

private:
    // static IndexConfigPtr CreateIndexConfig();
    static std::string GetIndexName(const autil::legacy::json::JsonMap& index);
    static std::string GetAnalyzerName(const autil::legacy::json::JsonMap& index);
    static InvertedIndexType GetInvertedIndexType(const autil::legacy::json::JsonMap& index, TableType tableType);
    static std::shared_ptr<DictionaryConfig> GetDictConfig(const autil::legacy::json::JsonMap& index,
                                                           const DictionarySchemaPtr& dictSchema);
    static std::shared_ptr<AdaptiveDictionaryConfig>
    GetAdaptiveDictConfig(const autil::legacy::json::JsonMap& index,
                          const AdaptiveDictionarySchemaPtr& adaptiveDictSchema);

    static std::shared_ptr<FileCompressConfig>
    GetFileCompressConfig(const autil::legacy::json::JsonMap& index,
                          const std::shared_ptr<FileCompressSchema>& fileCompressSchema);

    static void LoadPackageIndexFields(const autil::legacy::json::JsonMap& index, const std::string& indexName,
                                       std::vector<std::string>& fields, std::vector<int32_t>& boosts);
    static void LoadSingleFieldIndexField(const autil::legacy::json::JsonMap& index, const std::string& indexName,
                                          std::string& fieldName);
    static void CheckOptionFlag(InvertedIndexType indexType, const autil::legacy::json::JsonMap& index);
    static optionflag_t CalcOptionFlag(const autil::legacy::json::JsonMap& indexMap, InvertedIndexType indexType);

    static format_versionid_t GetFormatVersion(const autil::legacy::json::JsonMap& indexMap, bool isLoadFromIndex);

    static void updateOptionFlag(const autil::legacy::json::JsonMap& indexMap, const std::string& optionType,
                                 uint8_t flag, optionflag_t& option);

    static bool GetShardingConfigInfo(const autil::legacy::json::JsonMap& index, InvertedIndexType indexType,
                                      size_t& shardingCount);

    static IndexConfigPtr CreateKKVIndexConfig(const std::string& indexName, InvertedIndexType indexType,
                                               const autil::legacy::Any& indexConfigJson,
                                               const FieldSchemaPtr& fieldSchema);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexConfigCreator);
}} // namespace indexlib::config

#endif //__INDEXLIB_INDEX_CONFIG_CREATOR_H
