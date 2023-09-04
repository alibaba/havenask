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
#include "indexlib/index/inverted_index/config/InvertedIndexConfigSerializer.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/config/TruncateIndexConfig.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexConfigSerializer);

namespace {

void updateOptionFlag(const autil::legacy::json::JsonMap& jsonMap, const std::string& optionType, uint8_t flag,
                      optionflag_t* option)
{
    auto iter = jsonMap.find(optionType);
    if (iter != jsonMap.end()) {
        int optionFlag = autil::legacy::json::JsonNumberCast<int>(iter->second);
        if (optionFlag != 0) {
            *option |= flag;
        } else {
            *option &= (~flag);
        }
    }
}

} // namespace

void InvertedIndexConfigSerializer::Serialize(const indexlibv2::config::InvertedIndexConfig& indexConfig,
                                              autil::legacy::Jsonizable::JsonWrapper* json)
{
    if (indexConfig.IsVirtual()) {
        return;
    }
    assert(json->GetMode() == autil::legacy::Jsonizable::TO_JSON);
    std::map<std::string, autil::legacy::Any> jsonMap = json->GetMap();
    json->Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_NAME, indexConfig.GetIndexName());
    json->Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_FORMAT_VERSIONID,
                  indexConfig.GetIndexFormatVersionId());
    if (indexConfig.IsIndexUpdatable()) {
        json->Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_UPDATABLE, indexConfig.IsIndexUpdatable());
        if (indexConfig.IsPatchCompressed()) {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::PATCH_COMPRESSED, indexConfig.IsPatchCompressed());
        }
    }
    if (!indexConfig.GetAnalyzer().empty()) {
        json->Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_ANALYZER, indexConfig.GetAnalyzer());
    }
    std::string typeStr =
        indexlibv2::config::InvertedIndexConfig::InvertedIndexTypeToStr(indexConfig.GetInvertedIndexType());
    json->Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_TYPE, typeStr);

    int enableValue = 1;
    int disableValue = 0;

    if (indexConfig.GetDictConfig().get()) {
        std::string dictName = indexConfig.GetDictConfig()->GetDictName();
        json->Jsonize(indexlibv2::config::InvertedIndexConfig::HIGH_FEQUENCY_DICTIONARY, dictName);
    }

    if (indexConfig.GetAdaptiveDictionaryConfig().get()) {
        std::string ruleName = indexConfig.GetAdaptiveDictionaryConfig()->GetRuleName();
        json->Jsonize(indexlibv2::config::InvertedIndexConfig::HIGH_FEQUENCY_ADAPTIVE_DICTIONARY, ruleName);
    }

    if (indexConfig.GetFileCompressConfigV2().get()) {
        std::string fileCompress = indexConfig.GetFileCompressConfigV2()->GetCompressName();
        json->Jsonize(FILE_COMPRESSOR, fileCompress);
    }

    if (indexConfig.GetDictConfig() || indexConfig.GetAdaptiveDictionaryConfig()) {
        std::string postingType;
        if (indexConfig.GetHighFrequencyTermPostingType() == indexlib::index::hp_bitmap) {
            postingType = indexlibv2::config::InvertedIndexConfig::HIGH_FREQ_TERM_BITMAP_POSTING;
        } else {
            assert(indexConfig.GetHighFrequencyTermPostingType() == indexlib::index::hp_both);
            postingType = indexlibv2::config::InvertedIndexConfig::HIGH_FREQ_TERM_BOTH_POSTING;
        }
        json->Jsonize(indexlibv2::config::InvertedIndexConfig::HIGH_FEQUENCY_TERM_POSTING_TYPE, postingType);
    }

    if (indexConfig.GetShardingType() == indexlibv2::config::InvertedIndexConfig::IST_NEED_SHARDING) {
        int32_t shardingCount = (int32_t)indexConfig.GetShardingIndexConfigs().size();
        json->Jsonize(indexlibv2::config::InvertedIndexConfig::SHARDING_COUNT, shardingCount);
    }

    auto invertedIndexType = indexConfig.GetInvertedIndexType();
    auto optionFlag = indexConfig.GetOptionFlag();
    if (invertedIndexType == it_pack || invertedIndexType == it_expack || invertedIndexType == it_text) {
        if (optionFlag & of_term_payload) {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::TERM_PAYLOAD_FLAG, enableValue);
        } else {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::TERM_PAYLOAD_FLAG, disableValue);
        }

        if (optionFlag & of_doc_payload) {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::DOC_PAYLOAD_FLAG, enableValue);
        } else {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::DOC_PAYLOAD_FLAG, disableValue);
        }

        if (optionFlag & of_position_payload) {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::POSITION_PAYLOAD_FLAG, enableValue);
        } else {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::POSITION_PAYLOAD_FLAG, disableValue);
        }

        if (optionFlag & of_position_list) {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::POSITION_LIST_FLAG, enableValue);
        } else {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::POSITION_LIST_FLAG, disableValue);
        }

        if (optionFlag & of_tf_bitmap) {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::TERM_FREQUENCY_BITMAP, enableValue);
        } else {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::TERM_FREQUENCY_BITMAP, disableValue);
        }
    } else if (invertedIndexType == it_string || invertedIndexType == it_number ||
               invertedIndexType == it_number_int8 || invertedIndexType == it_number_uint8 ||
               invertedIndexType == it_number_int16 || invertedIndexType == it_number_uint16 ||
               invertedIndexType == it_number_int32 || invertedIndexType == it_number_uint32 ||
               invertedIndexType == it_number_int64 || invertedIndexType == it_number_uint64) {
        if (optionFlag & of_term_payload) {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::TERM_PAYLOAD_FLAG, enableValue);
        } else {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::TERM_PAYLOAD_FLAG, disableValue);
        }

        if (optionFlag & of_doc_payload) {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::DOC_PAYLOAD_FLAG, enableValue);
        } else {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::DOC_PAYLOAD_FLAG, disableValue);
        }
    }
    if ((invertedIndexType != it_primarykey64) && (invertedIndexType != it_primarykey128) &&
        (invertedIndexType != it_kv) && (invertedIndexType != it_kkv) && (invertedIndexType != it_trie) &&
        (invertedIndexType != it_spatial) && (invertedIndexType != it_datetime) && (invertedIndexType != it_range) &&
        !(optionFlag & of_term_frequency)) {
        json->Jsonize(indexlibv2::config::InvertedIndexConfig::TERM_FREQUENCY_FLAG, disableValue);
    }
    if (indexConfig.IsReferenceCompress()) {
        std::string indexCompressMode = indexlibv2::config::InvertedIndexConfig::INDEX_COMPRESS_MODE_REFERENCE;
        json->Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_COMPRESS_MODE, indexCompressMode);
    }

    if (indexConfig.IsHashTypedDictionary()) {
        json->Jsonize(indexlibv2::config::InvertedIndexConfig::USE_HASH_DICTIONARY,
                      indexConfig.IsHashTypedDictionary());
    }
    // truncate
    if (indexConfig.HasTruncate()) {
        json->Jsonize(indexlibv2::config::InvertedIndexConfig::HAS_TRUNCATE, indexConfig.HasTruncate());
        auto truncateProfilesStr = indexConfig.GetUseTruncateProfilesStr();
        if (!truncateProfilesStr.empty()) {
            json->Jsonize(indexlibv2::config::InvertedIndexConfig::USE_TRUNCATE_PROFILES, truncateProfilesStr);
        }
    }

    JsonizeShortListVbyteCompress(indexConfig, json);
}

void InvertedIndexConfigSerializer::DeserializeCommonFields(const autil::legacy::Any& any,
                                                            indexlibv2::config::InvertedIndexConfig* indexConfig)
{
    autil::legacy::Jsonizable::JsonWrapper json(any);
    std::string postingType = indexlibv2::config::InvertedIndexConfig::HIGH_FREQ_TERM_BITMAP_POSTING;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::HIGH_FEQUENCY_TERM_POSTING_TYPE, postingType, postingType);
    bool isIndexUpdatable = false;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_UPDATABLE, isIndexUpdatable, isIndexUpdatable);
    indexConfig->SetIndexUpdatable(isIndexUpdatable);
    bool isPatchCompressed = false;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::PATCH_COMPRESSED, isPatchCompressed, isPatchCompressed);
    indexConfig->SetPatchCompressed(isPatchCompressed);
    if (postingType == indexlibv2::config::InvertedIndexConfig::HIGH_FREQ_TERM_BOTH_POSTING) {
        indexConfig->SetHighFreqencyTermPostingType(indexlib::index::hp_both);
    } else if (postingType == indexlibv2::config::InvertedIndexConfig::HIGH_FREQ_TERM_BITMAP_POSTING) {
        indexConfig->SetHighFreqencyTermPostingType(indexlib::index::hp_bitmap);
    } else {
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "High frequency term posting list"
                             " type(%s) doesn't support.",
                             postingType.c_str());
    }

    std::string compressMode;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_COMPRESS_MODE, compressMode,
                 indexlibv2::config::InvertedIndexConfig::INDEX_COMPRESS_MODE_PFOR_DELTA);
    indexConfig->SetIsReferenceCompress(
        (compressMode == indexlibv2::config::InvertedIndexConfig::INDEX_COMPRESS_MODE_REFERENCE) ? true : false);
    bool isHashTypedDictionary = false;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::USE_HASH_DICTIONARY, isHashTypedDictionary,
                 isHashTypedDictionary);
    indexConfig->SetHashTypedDictionary(isHashTypedDictionary);
    // truncate
    bool hasTruncate = false;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::HAS_TRUNCATE, hasTruncate, hasTruncate);
    indexConfig->SetHasTruncateFlag(hasTruncate);

    if (indexConfig->HasTruncate()) {
        std::string useTruncateProfile;
        json.Jsonize(indexlibv2::config::InvertedIndexConfig::USE_TRUNCATE_PROFILES, useTruncateProfile,
                     useTruncateProfile);
        indexConfig->SetUseTruncateProfilesStr(useTruncateProfile);
    }

    JsonizeShortListVbyteCompress(json, indexConfig);
}

void InvertedIndexConfigSerializer::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                                const indexlibv2::config::IndexConfigDeserializeResource& resource,
                                                indexlibv2::config::InvertedIndexConfig* indexConfig)
{
    indexConfig->SetIndexId(idxInJsonArray);
    autil::legacy::Jsonizable::JsonWrapper json(any);
    std::string indexName;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_NAME, indexName);
    indexConfig->SetIndexName(indexName);
    std::string analyzer;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_ANALYZER, analyzer, analyzer);
    indexConfig->SetAnalyzer(analyzer);
    std::string invertedIndexTypeStr;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_TYPE, invertedIndexTypeStr);
    auto [status, invertedIndexType] = indexlibv2::config::InvertedIndexConfig::StrToIndexType(invertedIndexTypeStr);
    THROW_IF_STATUS_ERROR(status);
    indexConfig->SetInvertedIndexType(invertedIndexType);
    DeserializeOptionFlag(json, indexConfig);
    DeserializeIndexFormatVersion(json, indexConfig);
    DeserializeCommonFields(any, indexConfig);

    indexConfig->DoDeserialize(any, resource);

    auto dictConfig = DeserializeDictConfig<indexlib::config::DictionaryConfig>(json, resource);
    indexConfig->SetDictConfig(dictConfig);
    auto adaptiveDictConfig = DeserializeDictConfig<indexlib::config::AdaptiveDictionaryConfig>(json, resource);
    indexConfig->SetAdaptiveDictConfig(adaptiveDictConfig);
    DeserializeFileCompressConfig(json, resource, indexConfig);

    size_t shardingCount = 1;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::SHARDING_COUNT, shardingCount, shardingCount);
    if (shardingCount < 1) {
        INDEXLIB_FATAL_ERROR(Schema, "sharding count[%lu] invalid", shardingCount);
    }
    if (shardingCount > 1) {
        auto invertedIndexType = indexConfig->GetInvertedIndexType();
        if (invertedIndexType == it_primarykey64 || invertedIndexType == it_primarykey128 ||
            invertedIndexType == it_range || invertedIndexType == it_trie || invertedIndexType == it_spatial) {
            INDEXLIB_FATAL_ERROR(Schema, "inverted index type [%s] not support sharding[%lu]",
                                 invertedIndexTypeStr.c_str(), shardingCount);
        }
        const auto& indexName = indexConfig->GetIndexName();
        indexConfig->SetShardingType(indexlibv2::config::InvertedIndexConfig::IST_NEED_SHARDING);
        for (size_t i = 0; i < shardingCount; ++i) {
            std::shared_ptr<indexlibv2::config::InvertedIndexConfig> shardingIndexConfig(indexConfig->Clone());
            shardingIndexConfig->SetShardingType(indexlibv2::config::InvertedIndexConfig::IST_IS_SHARDING);
            shardingIndexConfig->SetIndexName(indexConfig->GetShardingIndexName(indexName, i));
            shardingIndexConfig->SetDictConfig(indexConfig->GetDictConfig());
            shardingIndexConfig->SetAdaptiveDictConfig(indexConfig->GetAdaptiveDictionaryConfig());
            shardingIndexConfig->SetFileCompressConfigV2(indexConfig->GetFileCompressConfigV2());
            indexConfig->AppendShardingIndexConfig(shardingIndexConfig);
        }
    }
}

void InvertedIndexConfigSerializer::CheckOptionFlag(const autil::legacy::json::JsonMap& jsonMap,
                                                    InvertedIndexType invertedIndexType)
{
    assert(invertedIndexType != it_kv && invertedIndexType != it_kkv);
    std::string strIndexType(indexlibv2::config::InvertedIndexConfig::InvertedIndexTypeToStr(invertedIndexType));
    if (invertedIndexType == it_string || invertedIndexType == it_number || invertedIndexType == it_primarykey64 ||
        invertedIndexType == it_primarykey128 || invertedIndexType == it_trie || invertedIndexType == it_spatial ||
        invertedIndexType == it_customized) {
        if (jsonMap.find(indexlibv2::config::InvertedIndexConfig::POSITION_LIST_FLAG) != jsonMap.end()) {
            std::stringstream ss;
            ss << strIndexType << " index cannot configure position_list_flag.";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }

        if (jsonMap.find(indexlibv2::config::InvertedIndexConfig::POSITION_PAYLOAD_FLAG) != jsonMap.end()) {
            std::stringstream ss;
            ss << strIndexType << " index cannot configure position_payload_flag.";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
    }

    if (invertedIndexType == it_primarykey64 || invertedIndexType == it_primarykey128 || invertedIndexType == it_trie ||
        invertedIndexType == it_spatial || invertedIndexType == it_customized) {
        if (jsonMap.find(indexlibv2::config::InvertedIndexConfig::DOC_PAYLOAD_FLAG) != jsonMap.end()) {
            std::stringstream ss;
            ss << strIndexType << " index cannot configure doc_payload_flag.";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }

        if (jsonMap.find(indexlibv2::config::InvertedIndexConfig::TERM_PAYLOAD_FLAG) != jsonMap.end()) {
            std::stringstream ss;
            ss << strIndexType << " index cannot configure term_payload_flag.";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
    }
}

void InvertedIndexConfigSerializer::DeserializeOptionFlag(const autil::legacy::Jsonizable::JsonWrapper& json,
                                                          indexlibv2::config::InvertedIndexConfig* indexConfig)
{
    auto invertedIndexType = indexConfig->GetInvertedIndexType();
    const auto& indexMap = json.GetMap();
    CheckOptionFlag(indexMap, invertedIndexType);

    optionflag_t optionFlag = (of_position_list | of_term_frequency);
    updateOptionFlag(indexMap, indexlibv2::config::InvertedIndexConfig::TERM_PAYLOAD_FLAG, of_term_payload,
                     &optionFlag);
    updateOptionFlag(indexMap, indexlibv2::config::InvertedIndexConfig::DOC_PAYLOAD_FLAG, of_doc_payload, &optionFlag);
    updateOptionFlag(indexMap, indexlibv2::config::InvertedIndexConfig::POSITION_PAYLOAD_FLAG, of_position_payload,
                     &optionFlag);
    updateOptionFlag(indexMap, indexlibv2::config::InvertedIndexConfig::POSITION_LIST_FLAG, of_position_list,
                     &optionFlag);
    updateOptionFlag(indexMap, indexlibv2::config::InvertedIndexConfig::TERM_FREQUENCY_FLAG, of_term_frequency,
                     &optionFlag);
    updateOptionFlag(indexMap, indexlibv2::config::InvertedIndexConfig::TERM_FREQUENCY_BITMAP, of_tf_bitmap,
                     &optionFlag);

    if (invertedIndexType == it_expack) {
        optionFlag |= of_fieldmap;
    }

    if (((optionFlag & of_position_list) == 0) && (optionFlag & of_position_payload)) {
        INDEXLIB_FATAL_ERROR(Schema, "position_payload_flag should not"
                                     " be set while position_list_flag is not set");
    }

    if ((optionFlag & of_tf_bitmap) && (invertedIndexType != it_pack && invertedIndexType != it_expack)) {
        // only pack & expack support term_frequency_bitmap
        optionFlag &= (~of_tf_bitmap);
    }

    if (invertedIndexType == it_primarykey128 || invertedIndexType == it_primarykey64 || invertedIndexType == it_trie ||
        invertedIndexType == it_spatial || invertedIndexType == it_datetime || invertedIndexType == it_range) {
        optionFlag = of_none;
    } else if (invertedIndexType == it_string || invertedIndexType == it_number) {
        optionFlag &= (~of_position_list);
    }

    if ((optionFlag & of_position_list) && ((optionFlag & of_term_frequency) == 0)) {
        INDEXLIB_FATAL_ERROR(Schema, "position_list_flag should not be set while "
                                     "term_frequency_flag is not set");
    }

    if ((optionFlag & of_tf_bitmap) && (!(optionFlag & of_term_frequency))) {
        INDEXLIB_FATAL_ERROR(Schema, "position_ft_bitmap should not be"
                                     " set while term_frequency_flag is not set");
    }
    indexConfig->SetOptionFlag(optionFlag);
}

void InvertedIndexConfigSerializer::DeserializeIndexFormatVersion(autil::legacy::Jsonizable::JsonWrapper& json,
                                                                  indexlibv2::config::InvertedIndexConfig* indexConfig)
{
    format_versionid_t formatVersionId = indexlibv2::config::InvertedIndexConfig::DEFAULT_FORMAT_VERSION;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::INDEX_FORMAT_VERSIONID, formatVersionId, formatVersionId);
    auto status = indexConfig->SetIndexFormatVersionId(formatVersionId);
    THROW_IF_STATUS_ERROR(status);
}

template <typename DictConfigType>
std::shared_ptr<DictConfigType>
InvertedIndexConfigSerializer::DeserializeDictConfig(const autil::legacy::Jsonizable::JsonWrapper& json,
                                                     const indexlibv2::config::IndexConfigDeserializeResource& resource)
{
    const auto& jsonMap = json.GetMap();
    std::string jsonKey;
    std::string dictKey;
    if constexpr (std::is_same_v<DictConfigType, indexlib::config::DictionaryConfig>) {
        jsonKey = indexlibv2::config::InvertedIndexConfig::HIGH_FEQUENCY_DICTIONARY;
        dictKey = indexlibv2::config::InvertedIndexConfig::DICTIONARIES;
    } else {
        jsonKey = indexlibv2::config::InvertedIndexConfig::HIGH_FEQUENCY_ADAPTIVE_DICTIONARY;
        dictKey = indexlibv2::config::InvertedIndexConfig::ADAPTIVE_DICTIONARIES;
    }
    auto iter = jsonMap.find(jsonKey);
    if (iter == jsonMap.end()) {
        return nullptr;
    }

    std::string dictName = autil::legacy::AnyCast<std::string>(iter->second);
    if (dictName.empty()) {
        return nullptr;
    }
    auto dictionaries = resource.GetRuntimeSetting<std::shared_ptr<std::vector<DictConfigType>>>(dictKey);
    if (!dictionaries || !*dictionaries) {
        INDEXLIB_FATAL_ERROR(Schema, "get [%s] from settings failed", dictKey.c_str());
    }
    for (const auto& dictConfig : **dictionaries) {
        if constexpr (std::is_same_v<DictConfigType, indexlib::config::DictionaryConfig>) {
            if (dictConfig.GetDictName() == dictName) {
                return std::make_shared<DictConfigType>(dictConfig);
            }
        } else {
            if (dictConfig.GetRuleName() == dictName) {
                return std::make_shared<DictConfigType>(dictConfig);
            }
        }
    }
    INDEXLIB_FATAL_ERROR(Schema, "get dict [%s] from settings failed", dictName.c_str());
}

void InvertedIndexConfigSerializer::DeserializeFileCompressConfig(
    const autil::legacy::Jsonizable::JsonWrapper& json,
    const indexlibv2::config::IndexConfigDeserializeResource& resource,
    indexlibv2::config::InvertedIndexConfig* indexConfig)
{
    const auto& jsonMap = json.GetMap();
    auto iter = jsonMap.find(FILE_COMPRESSOR);
    if (iter == jsonMap.end()) {
        return;
    }
    std::string compressName = autil::legacy::AnyCast<std::string>(iter->second);
    if (compressName.empty()) {
        return;
    }
    auto fileCompressConfig = resource.GetFileCompressConfig(compressName);
    if (!fileCompressConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "get file compress[%s] failed", compressName.c_str());
    }
    indexConfig->SetFileCompressConfigV2(fileCompressConfig);
}

void InvertedIndexConfigSerializer::JsonizeShortListVbyteCompress(
    const indexlibv2::config::InvertedIndexConfig& indexConfig, autil::legacy::Jsonizable::JsonWrapper* json)
{
    auto invertedIndexType = indexConfig.GetInvertedIndexType();
    if (invertedIndexType == it_primarykey64 || invertedIndexType == it_primarykey128 || invertedIndexType == it_kv ||
        invertedIndexType == it_kkv || invertedIndexType == it_trie || invertedIndexType == it_customized) {
        return;
    }

    assert(json->GetMode() == autil::legacy::Jsonizable::TO_JSON);
    json->Jsonize(indexlibv2::config::InvertedIndexConfig::HAS_SHORTLIST_VBYTE_COMPRESS,
                  indexConfig.IsShortListVbyteCompress());
}

void InvertedIndexConfigSerializer::JsonizeShortListVbyteCompress(autil::legacy::Jsonizable::JsonWrapper& json,
                                                                  indexlibv2::config::InvertedIndexConfig* indexConfig)
{
    auto invertedIndexType = indexConfig->GetInvertedIndexType();
    if (invertedIndexType == it_primarykey64 || invertedIndexType == it_primarykey128 || invertedIndexType == it_kv ||
        invertedIndexType == it_kkv || invertedIndexType == it_trie || invertedIndexType == it_customized) {
        return;
    }

    bool defaultValue = (indexConfig->GetIndexFormatVersionId() >= 1) ? true : false;
    assert(json.GetMode() == autil::legacy::Jsonizable::FROM_JSON);
    bool compress = false;
    json.Jsonize(indexlibv2::config::InvertedIndexConfig::HAS_SHORTLIST_VBYTE_COMPRESS, compress, defaultValue);
    indexConfig->SetShortListVbyteCompress(compress);
}

} // namespace indexlib::index
