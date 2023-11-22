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
#include "indexlib/config/legacy_schema_adapter.h"

#include "indexlib/config/MutableJson.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/index/ann/Common.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/table/normal_table/Common.h"

namespace indexlib::config {

AUTIL_LOG_SETUP(indexlib.config, LegacySchemaAdapter);

LegacySchemaAdapter::LegacySchemaAdapter(const std::shared_ptr<IndexPartitionSchema>& legacySchema)
    : _legacySchema(legacySchema)
{
    // legacySchema would be empty in some test
    if (_legacySchema) {
        Init();
    }
}

const std::string& LegacySchemaAdapter::GetTableType() const { return _legacySchema->GetTableTypeV2(); }

const std::string& LegacySchemaAdapter::GetTableName() const { return _legacySchema->GetSchemaName(); }

schemaid_t LegacySchemaAdapter::GetSchemaId() const { return _legacySchema->GetSchemaVersionId(); }

std::string LegacySchemaAdapter::GetSchemaFileName() const
{
    return IndexPartitionSchema::GetSchemaFileName(GetSchemaId());
}

std::shared_ptr<indexlibv2::config::FieldConfig> LegacySchemaAdapter::GetFieldConfig(const std::string& fieldName) const
{
    return _legacySchema->GetFieldConfig(fieldName);
}

fieldid_t LegacySchemaAdapter::GetFieldId(const std::string& fieldName) const
{
    return _legacySchema->GetFieldId(fieldName);
}

std::shared_ptr<indexlibv2::config::FieldConfig> LegacySchemaAdapter::GetFieldConfig(fieldid_t fieldId) const
{
    return _legacySchema->GetFieldConfig(fieldId);
}

size_t LegacySchemaAdapter::GetFieldCount() const { return _legacySchema->GetFieldCount(); }

std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> LegacySchemaAdapter::GetFieldConfigs() const
{
    return FieldConfig::FieldConfigsToV2(_legacySchema->GetFieldConfigs());
}

std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>>
LegacySchemaAdapter::GetIndexFieldConfigs(const std::string& indexType) const
{
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> ret;
    std::map<fieldid_t, std::shared_ptr<indexlibv2::config::FieldConfig>> fields;
    auto indexConfigs = GetIndexConfigs(indexType);
    for (const auto& indexConfig : indexConfigs) {
        if (indexConfig->IsDisabled()) {
            continue;
        }
        auto indexFields = indexConfig->GetFieldConfigs();
        for (const auto& fieldConfig : indexFields) {
            fields[fieldConfig->GetFieldId()] = fieldConfig;
        }
    }
    for (const auto& [fieldId, fieldConfig] : fields) {
        ret.push_back(fieldConfig);
    }
    return ret;
}

LegacySchemaAdapter::IIndexConfigVector LegacySchemaAdapter::GetIndexConfigs(const std::string& indexType) const
{
    if (indexType == indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR) {
        return _primaryKeyIndexConfigs;
    } else if (indexType == indexlib::index::INVERTED_INDEX_TYPE_STR) {
        return _invertedIndexConfigs;
    } else if (indexType == indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR) {
        return _attributeConfigs;
    } else if (indexType == indexlibv2::index::PACK_ATTRIBUTE_INDEX_TYPE_STR) {
        return _packAttributeConfigs;
    } else if (indexType == indexlibv2::index::ANN_INDEX_TYPE_STR) {
        return _annIndexConfigs;
    } else if (indexType == indexlibv2::table::VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR) {
        return _virtualAttributeConfigs;
    } else if (indexType == indexlibv2::index::SUMMARY_INDEX_TYPE_STR) {
        return _summaryIndexConfigs;
    } else if (indexType == indexlibv2::index::KV_INDEX_TYPE_STR) {
        return _kvIndexConfigs;
    } else if (indexType == indexlibv2::index::KKV_INDEX_TYPE_STR) {
        return _kkvIndexConfigs;
    } else if (indexType == indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR) {
        return _generalInvertedIndexConfigs;
    } else if (indexType == indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR) {
        return _generalValueConfigs;
    } else {
        return {};
    }
}

LegacySchemaAdapter::IIndexConfigVector LegacySchemaAdapter::GetIndexConfigs() const
{
    assert(false); // never got hear
    LegacySchemaAdapter::IIndexConfigVector indexConfigVector(_primaryKeyIndexConfigs);
    indexConfigVector.insert(indexConfigVector.end(), _invertedIndexConfigs.begin(), _invertedIndexConfigs.end());
    indexConfigVector.insert(indexConfigVector.end(), _annIndexConfigs.begin(), _annIndexConfigs.end());
    indexConfigVector.insert(indexConfigVector.end(), _attributeConfigs.begin(), _attributeConfigs.end());
    indexConfigVector.insert(indexConfigVector.end(), _packAttributeConfigs.begin(), _packAttributeConfigs.end());
    indexConfigVector.insert(indexConfigVector.end(), _virtualAttributeConfigs.begin(), _virtualAttributeConfigs.end());
    indexConfigVector.insert(indexConfigVector.end(), _summaryIndexConfigs.begin(), _summaryIndexConfigs.end());
    indexConfigVector.insert(indexConfigVector.end(), _kvIndexConfigs.begin(), _kvIndexConfigs.end());
    indexConfigVector.insert(indexConfigVector.end(), _kkvIndexConfigs.begin(), _kkvIndexConfigs.end());
    return indexConfigVector;
}

LegacySchemaAdapter::IIndexConfigVector
LegacySchemaAdapter::GetIndexConfigsByFieldName(const std::string& fieldName) const
{
    assert(false); // never got hear
    auto indexConfigs = GetIndexConfigs();
    indexConfigs.erase(std::remove_if(indexConfigs.begin(), indexConfigs.end(),
                                      [&fieldName](const std::shared_ptr<IIndexConfig>& indexConfig) {
                                          const auto& fieldConfigs = indexConfig->GetFieldConfigs();
                                          for (const auto& fieldConfig : fieldConfigs) {
                                              if (fieldConfig->GetFieldName() == fieldName) {
                                                  return false;
                                              }
                                          }
                                          return true;
                                      }),
                       indexConfigs.end());
    return indexConfigs;
}

std::shared_ptr<LegacySchemaAdapter::IIndexConfig>
LegacySchemaAdapter::GetIndexConfig(const std::string& indexType, const std::string& indexName) const
{
    auto iter = _indexConfigMap.find({indexType, indexName});
    if (iter == _indexConfigMap.end()) {
        return nullptr;
    }
    assert(!iter->second->IsDisabled());
    return iter->second;
}

std::shared_ptr<indexlibv2::config::IIndexConfig> LegacySchemaAdapter::GetPrimaryKeyIndexConfig() const
{
    return _primaryKeyIndexConfig;
}

void LegacySchemaAdapter::Init()
{
    const auto& indexSchema = _legacySchema->GetIndexSchema();
    const auto& attrSchema = _legacySchema->GetAttributeSchema();
    const auto& virtualAttrSchema = _legacySchema->GetVirtualAttributeSchema();
    const auto& summarySchema = _legacySchema->GetSummarySchema();

    if (indexSchema) {
        for (auto iter = indexSchema->Begin(); iter != indexSchema->End(); ++iter) {
            const auto& legacyConfig = *iter;
            auto indexType = legacyConfig->GetInvertedIndexType();
            if (indexType == it_kv || indexType == it_kkv) {
                continue;
            }
            std::shared_ptr<indexlibv2::config::InvertedIndexConfig> configV2 = legacyConfig->ConstructConfigV2();
            if (configV2 && !configV2->IsDisabled()) {
                _generalInvertedIndexConfigs.push_back(configV2);
                AddIndexConfigToMap(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, configV2);
                if (configV2->GetIndexType() == indexlib::index::INVERTED_INDEX_TYPE_STR) {
                    _invertedIndexConfigs.push_back(configV2);
                } else if (configV2->GetIndexType() == indexlibv2::index::ANN_INDEX_TYPE_STR) {
                    _annIndexConfigs.push_back(configV2);
                } else if (configV2->GetIndexType() == indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR) {
                    continue;
                } else {
                    // trie?
                }
                AddIndexConfigToMap(configV2->GetIndexType(), configV2);
            }
        }
        const auto& pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
        if (pkConfig) {
            assert(!pkConfig->IsDisabled());
            if (_legacySchema->GetTableType() == tt_kv) {
                const auto& legacyKvIndexConfig = std::dynamic_pointer_cast<indexlib::config::KVIndexConfig>(pkConfig);
                auto kvConfig = std::shared_ptr<indexlibv2::config::KVIndexConfig>(
                    legacyKvIndexConfig->MakeKVIndexConfigV2().release());
                assert(kvConfig);
                _kvIndexConfigs.emplace_back(kvConfig);
                _primaryKeyIndexConfig = kvConfig;
                AddIndexConfigToMap(indexlibv2::index::KV_INDEX_TYPE_STR, kvConfig);
                //_primaryKeyIndexConfigs.push_back(kvConfig);
                // AddIndexConfigToMap(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR, kvConfig);
            } else if (_legacySchema->GetTableType() == tt_kkv) {
                const auto& legacyKkvIndexConfig =
                    std::dynamic_pointer_cast<indexlib::config::KKVIndexConfig>(pkConfig);
                auto kkvConfig = std::shared_ptr<indexlibv2::config::KKVIndexConfig>(
                    legacyKkvIndexConfig->MakeKKVIndexConfigV2().release());
                assert(kkvConfig);
                _kkvIndexConfigs.emplace_back(kkvConfig);
                _primaryKeyIndexConfig = kkvConfig;
                AddIndexConfigToMap(indexlibv2::index::KKV_INDEX_TYPE_STR, kkvConfig);
                //_primaryKeyIndexConfigs.push_back(kkvConfig);
                // AddIndexConfigToMap(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR, kkvConfig);
            } else {
                auto pkConfigV2 =
                    std::shared_ptr<indexlibv2::config::InvertedIndexConfig>(pkConfig->ConstructConfigV2());
                assert(pkConfigV2);
                _primaryKeyIndexConfig = pkConfigV2;
                _primaryKeyIndexConfigs.push_back(pkConfigV2);
                AddIndexConfigToMap(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR, pkConfigV2);
            }
        }
    }

    if (attrSchema) {
        for (auto iter = attrSchema->Begin(); iter != attrSchema->End(); ++iter) {
            const auto& attrConfig = *iter;
            if (attrConfig->IsDisabled()) {
                continue;
            }
            _generalValueConfigs.push_back(attrConfig);
            AddIndexConfigToMap(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig);
            if (!attrConfig->GetPackAttributeConfig()) {
                _attributeConfigs.push_back(attrConfig);
                AddIndexConfigToMap(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR, attrConfig);
            }
        }
        for (size_t i = 0; i < attrSchema->GetPackAttributeCount(); ++i) {
            const auto& packAttrConfig = attrSchema->GetPackAttributeConfig(i);
            if (packAttrConfig->IsDisabled()) {
                continue;
            }
            _packAttributeConfigs.push_back(packAttrConfig);
            AddIndexConfigToMap(indexlibv2::index::PACK_ATTRIBUTE_INDEX_TYPE_STR, packAttrConfig);
        }
    }
    if (virtualAttrSchema) {
        for (auto iter = virtualAttrSchema->Begin(); iter != virtualAttrSchema->End(); ++iter) {
            assert(!(*iter)->IsDisabled());
            _virtualAttributeConfigs.push_back(*iter);
            AddIndexConfigToMap(indexlibv2::table::VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR, *iter);
        }
        assert(virtualAttrSchema->GetPackAttributeCount() == 0);
        // for (size_t i = 0; i < virtualAttrSchema->GetPackAttributeCount(); ++i) {
        //     const auto& packAttrConfig = virtualAttrSchema->GetPackAttributeConfig(i);
        //     assert(!iter->IsDisabled());
        //     _virtualAttributeConfigs.push_back(packAttrConfig);
        //     AddIndexConfigToMap(indexlibv2::table::VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR, packAttrConfig);
        // }
    }

    if (summarySchema) {
        std::shared_ptr<indexlibv2::config::SummaryIndexConfig> summaryIndexConfig(
            summarySchema->MakeSummaryIndexConfigV2().release());
        if (!summaryIndexConfig->IsDisabled()) {
            _summaryIndexConfigs.push_back(summaryIndexConfig);
            AddIndexConfigToMap(indexlibv2::index::SUMMARY_INDEX_TYPE_STR, summaryIndexConfig);
        }
    }
}

void LegacySchemaAdapter::AddIndexConfigToMap(const std::string& type, const std::shared_ptr<IIndexConfig>& indexConfig)
{
    assert(!indexConfig->IsDisabled());
    _indexConfigMap[std::make_pair(type, indexConfig->GetIndexName())] = indexConfig;
}

std::shared_ptr<indexlibv2::config::ITabletSchema> LegacySchemaAdapter::LoadLegacySchema(const std::string& jsonStr)
{
    try {
        auto legacySchema = std::make_shared<IndexPartitionSchema>();
        legacySchema->SetLoadFromIndex(false);
        autil::legacy::FromJsonString(*legacySchema, jsonStr);
        legacySchema->Check();
        auto adapter = std::make_shared<LegacySchemaAdapter>(legacySchema);
        return adapter;
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "load legacy schema failed, exception[%s]", e.what());
        return nullptr;
    }
}

const indexlibv2::config::MutableJson& LegacySchemaAdapter::GetRuntimeSettings() const
{
    static indexlibv2::config::MutableJson empty;
    assert(false);
    return empty;
}

const indexlib::util::JsonMap& LegacySchemaAdapter::GetUserDefinedParam() const
{
    return _legacySchema->GetUserDefinedParam();
}

bool LegacySchemaAdapter::Serialize(bool isCompact, std::string* output) const
{
    if (!_legacySchema) {
        return false;
    }
    *output = autil::legacy::ToJsonString(*_legacySchema, isCompact);
    return true;
}

} // namespace indexlib::config
