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
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

#include "autil/EnvUtil.h"
#include "autil/StringTokenizer.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/index/ann/Common.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfigSerializer.h"
#include "indexlib/index/inverted_index/config/PayloadConfig.h"
#include "indexlib/index/inverted_index/config/TruncateProfileConfig.h"
#include "indexlib/index/inverted_index/config/TruncateTermVocabulary.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, InvertedIndexConfig);

#define INDEX_FORMAT_VERSION_ENV_STR "INDEXLIB_DEFAULT_INVERTED_INDEX_FORMAT_VERSION"

const format_versionid_t InvertedIndexConfig::BINARY_FORMAT_VERSION = 1;
format_versionid_t InvertedIndexConfig::DEFAULT_FORMAT_VERSION =
    autil::EnvUtil::getEnv(INDEX_FORMAT_VERSION_ENV_STR, (format_versionid_t)0);

struct InvertedIndexConfig::Impl {
    indexid_t indexId = INVALID_INDEXID;
    indexid_t parentIndexId = INVALID_INDEXID;
    std::string indexName;
    InvertedIndexType invertedIndexType = it_unknown;
    std::string analyzer;
    std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDictConfig; // not copy
    std::shared_ptr<DictionaryConfig> dictConfig;                 // not copy
    std::shared_ptr<HighFrequencyVocabulary> highFreqVocabulary;  // not copy
    std::shared_ptr<FileCompressConfig> fileCompressConfig;
    std::shared_ptr<FileCompressConfigV2> fileCompressConfigV2;
    std::string useTruncateProfiles;
    indexlib::index::HighFrequencyTermPostingType highFrequencyTermPostingType = indexlib::index::hp_bitmap;
    std::shared_ptr<TruncateTermVocabulary> truncateTermVocabulary;
    optionflag_t optionFlag = OPTION_FLAG_ALL;
    InvertedIndexConfig::IndexShardingType shardingType = InvertedIndexConfig::IST_NO_SHARDING;
    std::vector<std::shared_ptr<InvertedIndexConfig>> shardingIndexConfigs; // not copy
    std::vector<std::shared_ptr<InvertedIndexConfig>> truncateIndexConfigs; // not copy
    indexlib::IndexStatus status = indexlib::is_normal;
    indexlib::schema_opid_t ownerOpId = indexlib::INVALID_SCHEMA_OP_ID;
    format_versionid_t formatVersionId = DEFAULT_FORMAT_VERSION;
    format_versionid_t maxSupportedFormatVersionId = BINARY_FORMAT_VERSION;
    uint32_t bloomFilterMultipleNum = 0;
    string nonTruncIndexName; // not copy
    bool isReferenceCompress = false;
    bool isHashTypedDictionary = false;
    bool isIndexUpdatable = false;
    bool isPatchCompressed = false;
    bool isVirtual = false;
    bool isShortListVbyteCompress = false;
    bool hasTruncate = false;
    indexlib::config::PayloadConfig payloadConfig;

    Impl() {}
    Impl(const std::string& indexName, InvertedIndexType invertedIndexType)
        : indexName(indexName)
        , invertedIndexType(invertedIndexType)
        , optionFlag(invertedIndexType == it_expack ? EXPACK_OPTION_FLAG_ALL : OPTION_FLAG_ALL)
    {
    }
    Impl(const Impl& other)
        : indexId(other.indexId)
        , parentIndexId(other.parentIndexId)
        , indexName(other.indexName)
        , invertedIndexType(other.invertedIndexType)
        , analyzer(other.analyzer)
        , fileCompressConfig(other.fileCompressConfig)
        , fileCompressConfigV2(other.fileCompressConfigV2)
        , useTruncateProfiles(other.useTruncateProfiles)
        , highFrequencyTermPostingType(other.highFrequencyTermPostingType)
        , optionFlag(other.optionFlag)
        , shardingType(other.shardingType)
        , status(other.status)
        , ownerOpId(other.ownerOpId)
        , formatVersionId(other.formatVersionId)
        , maxSupportedFormatVersionId(other.maxSupportedFormatVersionId)
        , bloomFilterMultipleNum(other.bloomFilterMultipleNum)
        , isReferenceCompress(other.isReferenceCompress)
        , isHashTypedDictionary(other.isHashTypedDictionary)
        , isIndexUpdatable(other.isIndexUpdatable)
        , isPatchCompressed(other.isPatchCompressed)
        , isVirtual(other.isVirtual)
        , isShortListVbyteCompress(other.isShortListVbyteCompress)
        , hasTruncate(other.hasTruncate)
    {
    }
};

InvertedIndexConfig::Iterator::Iterator() {}
InvertedIndexConfig::Iterator::Iterator(const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs)
    : _fieldConfigs(fieldConfigs)
{
}
InvertedIndexConfig::Iterator::Iterator(const std::shared_ptr<FieldConfig>& fieldConfig)
{
    _fieldConfigs.push_back(fieldConfig);
}
InvertedIndexConfig::Iterator::~Iterator() {}
bool InvertedIndexConfig::Iterator::HasNext() const { return _idx < _fieldConfigs.size(); }
std::shared_ptr<FieldConfig> InvertedIndexConfig::Iterator::Next()
{
    if (_idx < _fieldConfigs.size()) {
        return _fieldConfigs[_idx++];
    }
    return std::shared_ptr<FieldConfig>();
}

void InvertedIndexConfig::ResetDefaultFormatVersion()
{
    format_versionid_t targetId = autil::EnvUtil::getEnv(INDEX_FORMAT_VERSION_ENV_STR, (format_versionid_t)0);
    format_versionid_t currentId = InvertedIndexConfig::DEFAULT_FORMAT_VERSION;
    if (targetId != currentId) {
        cout << "reset inverted index format version "
             << "from " << currentId << " to " << targetId << endl;
        InvertedIndexConfig::DEFAULT_FORMAT_VERSION = targetId;
    }
}

InvertedIndexConfig::InvertedIndexConfig() : _impl(std::make_unique<Impl>()) {}

InvertedIndexConfig::InvertedIndexConfig(const string& indexName, InvertedIndexType invertedIndexType)
    : _impl(std::make_unique<Impl>(indexName, invertedIndexType))
{
}

InvertedIndexConfig::InvertedIndexConfig(const InvertedIndexConfig& other) : _impl(std::make_unique<Impl>(*other._impl))
{
}

InvertedIndexConfig& InvertedIndexConfig::operator=(const InvertedIndexConfig& other)
{
    if (&other != this) {
        _impl = std::make_unique<Impl>(*other._impl);
    }
    return *this;
}

InvertedIndexConfig::~InvertedIndexConfig() {}

int32_t InvertedIndexConfig::GetFieldIdxInPack(fieldid_t id) const { return -1; }
Status InvertedIndexConfig::CheckEqual(const InvertedIndexConfig& other) const
{
    CHECK_CONFIG_EQUAL(_impl->indexId, other._impl->indexId, "_impl->indexId not equal");
    CHECK_CONFIG_EQUAL(_impl->parentIndexId, other._impl->parentIndexId, "_impl->parentIndexId not equal");
    CHECK_CONFIG_EQUAL(_impl->indexName, other._impl->indexName, "_impl->indexName not equal");
    CHECK_CONFIG_EQUAL(_impl->invertedIndexType, other._impl->invertedIndexType, "_impl->invertedIndexType not equal");
    CHECK_CONFIG_EQUAL(_impl->shardingType, other._impl->shardingType, "_impl->shardingType not equal");
    CHECK_CONFIG_EQUAL(_impl->optionFlag, other._impl->optionFlag, "_impl->optionFlag not equal");
    CHECK_CONFIG_EQUAL(_impl->shardingIndexConfigs.size(), other._impl->shardingIndexConfigs.size(),
                       "_impl->shardingIndexConfigs size not equal");
    CHECK_CONFIG_EQUAL(_impl->formatVersionId, other._impl->formatVersionId, "_impl->formatVersionId not equal");
    CHECK_CONFIG_EQUAL(_impl->isShortListVbyteCompress, other._impl->isShortListVbyteCompress,
                       "_impl->isShortListVbyteCompress not equal");

    for (size_t i = 0; i < _impl->shardingIndexConfigs.size(); i++) {
        auto status = _impl->shardingIndexConfigs[i]->CheckEqual(*other._impl->shardingIndexConfigs[i]);
        RETURN_IF_STATUS_ERROR(status, "sharding index config [%s] not equal",
                               _impl->shardingIndexConfigs[i]->GetIndexName().c_str());
    }
    return Status::OK();
}
void InvertedIndexConfig::Check() const
{
    if (_impl->isIndexUpdatable) {
        bool isIndexUpdatable =
            (_impl->invertedIndexType == it_number) || (_impl->invertedIndexType == it_number_int8) ||
            (_impl->invertedIndexType == it_number_int16) || (_impl->invertedIndexType == it_number_int32) ||
            (_impl->invertedIndexType == it_number_int64) || (_impl->invertedIndexType == it_number_uint8) ||
            (_impl->invertedIndexType == it_number_uint16) || (_impl->invertedIndexType == it_number_uint32) ||
            (_impl->invertedIndexType == it_number_uint64) || (_impl->invertedIndexType == it_string);
        isIndexUpdatable = isIndexUpdatable && (_impl->optionFlag == 0);
        if (!isIndexUpdatable) {
            INDEXLIB_FATAL_ERROR(Schema, "index [%s] does not support [%s = true].", _impl->indexName.c_str(),
                                 INDEX_UPDATABLE.c_str());
        }
    }
    if ((_impl->optionFlag & of_position_payload) && !(_impl->optionFlag & of_position_list)) {
        INDEXLIB_FATAL_ERROR(Schema, "position payload flag is 1 but no position list. index name [%s]",
                             GetIndexName().c_str());
    }

    if (!_impl->dictConfig && !_impl->adaptiveDictConfig &&
        _impl->highFrequencyTermPostingType == indexlib::index::hp_both) {
        INDEXLIB_FATAL_ERROR(Schema,
                             "index[%s] error: high_frequency_term_posting_type is set to both"
                             " without high_frequency_dictionary[%d]/high_frequency_adaptive_dictionary[%d]",
                             _impl->indexName.c_str(), _impl->dictConfig ? 1 : 0, _impl->adaptiveDictConfig ? 1 : 0);
    }

    if (_impl->isReferenceCompress &&
        ((_impl->optionFlag & of_term_frequency) && !(_impl->optionFlag & of_tf_bitmap))) {
        INDEXLIB_FATAL_ERROR(Schema, "reference_compress does not support tf(not tf_bitmap), index name [%s]",
                             GetIndexName().c_str());
    }

    if (_impl->formatVersionId > _impl->maxSupportedFormatVersionId) {
        INDEXLIB_FATAL_ERROR(Schema, "format_verison_id [%d] over max supported value [%d], index name [%s]",
                             _impl->formatVersionId, _impl->maxSupportedFormatVersionId, GetIndexName().c_str());
    }

    bool supportFileCompress =
        (_impl->invertedIndexType == it_number) || (_impl->invertedIndexType == it_number_int8) ||
        (_impl->invertedIndexType == it_number_int16) || (_impl->invertedIndexType == it_number_int32) ||
        (_impl->invertedIndexType == it_number_int64) || (_impl->invertedIndexType == it_number_uint8) ||
        (_impl->invertedIndexType == it_number_uint16) || (_impl->invertedIndexType == it_number_uint32) ||
        (_impl->invertedIndexType == it_number_uint64) || (_impl->invertedIndexType == it_string) ||
        (_impl->invertedIndexType == it_text) || (_impl->invertedIndexType == it_pack) ||
        (_impl->invertedIndexType == it_expack) || (_impl->invertedIndexType == it_range) ||
        (_impl->invertedIndexType == it_date) || (_impl->invertedIndexType == it_spatial);
    if (_impl->fileCompressConfig && !_impl->fileCompressConfig->GetCompressType().empty() && !supportFileCompress) {
        INDEXLIB_FATAL_ERROR(Schema, "index [%s] with type [%s] not support enable file_compress",
                             _impl->indexName.c_str(),
                             InvertedIndexConfig::InvertedIndexTypeToStr(_impl->invertedIndexType));
    }
}

void InvertedIndexConfig::DoDeserialize(const autil::legacy::Any& any,
                                        const config::IndexConfigDeserializeResource& resource)
{
    indexlib::index::InvertedIndexConfigSerializer::DeserializeCommonFields(any, this);
}

void InvertedIndexConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                      const config::IndexConfigDeserializeResource& resource)
{
    indexlib::index::InvertedIndexConfigSerializer::Deserialize(any, idxInJsonArray, resource, this);
}
void InvertedIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    indexlib::index::InvertedIndexConfigSerializer::Serialize(*this, &json);
}

indexid_t InvertedIndexConfig::GetIndexId() const { return _impl->indexId; }
void InvertedIndexConfig::SetIndexId(indexid_t id)
{
    _impl->indexId = id;
    for (const auto& shardingConfig : _impl->shardingIndexConfigs) {
        shardingConfig->SetParentIndexId(id);
    }
}

const std::string& InvertedIndexConfig::GetIndexCommonPath() const { return indexlib::index::INVERTED_INDEX_PATH; }

std::vector<std::string> InvertedIndexConfig::GetIndexPath() const
{
    std::vector<std::string> paths;
    auto shardingType = GetShardingType();
    if (shardingType == IST_NO_SHARDING || shardingType == IST_IS_SHARDING) {
        paths.push_back(GetIndexCommonPath() + "/" + GetIndexName());
    }
    return paths;
}

const string& InvertedIndexConfig::GetIndexName() const { return _impl->indexName; }
void InvertedIndexConfig::SetIndexName(const string& indexName) { _impl->indexName = indexName; }

InvertedIndexType InvertedIndexConfig::GetInvertedIndexType() const { return _impl->invertedIndexType; }

const string& InvertedIndexConfig::GetIndexType() const
{
    InvertedIndexType invertedIndexType = _impl->invertedIndexType;
    switch (invertedIndexType) {
    case InvertedIndexType::it_primarykey64:
        return indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR;
    case InvertedIndexType::it_primarykey128:
        return indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR;
    case InvertedIndexType::it_text:
    case InvertedIndexType::it_pack:
    case InvertedIndexType::it_expack:
    case InvertedIndexType::it_string:
    case InvertedIndexType::it_number:      // use external -- legacy
    case InvertedIndexType::it_number_int8: // 8 - 64 use internal, type transform in InitIndexWriters
    case InvertedIndexType::it_number_uint8:
    case InvertedIndexType::it_number_int16:
    case InvertedIndexType::it_number_uint16:
    case InvertedIndexType::it_number_int32:
    case InvertedIndexType::it_number_uint32:
    case InvertedIndexType::it_number_int64:
    case InvertedIndexType::it_number_uint64:
    case InvertedIndexType::it_range:
    case InvertedIndexType::it_date:
    case InvertedIndexType::it_spatial:
        return indexlib::index::INVERTED_INDEX_TYPE_STR;
    case InvertedIndexType::it_customized:
        return indexlibv2::index::ANN_INDEX_TYPE_STR;
    default:
        static string DEFAULT = "unknown";
        return DEFAULT;
    }
}

void InvertedIndexConfig::SetInvertedIndexType(InvertedIndexType invertedIndexType)
{
    _impl->invertedIndexType = invertedIndexType;
}

const string& InvertedIndexConfig::GetAnalyzer() const { return _impl->analyzer; }
void InvertedIndexConfig::SetAnalyzer(const string& analyzerName) { _impl->analyzer = analyzerName; }

void InvertedIndexConfig::SetOptionFlag(optionflag_t optionFlag) { _impl->optionFlag = optionFlag; }
optionflag_t InvertedIndexConfig::GetOptionFlag() const { return _impl->optionFlag; }

bool InvertedIndexConfig::IsShortListVbyteCompress() const { return _impl->isShortListVbyteCompress; }
void InvertedIndexConfig::SetShortListVbyteCompress(bool isShortListVbyteCompress)
{
    _impl->isShortListVbyteCompress = isShortListVbyteCompress;
}

void InvertedIndexConfig::SetIsReferenceCompress(bool isReferenceCompress)
{
    _impl->isReferenceCompress = isReferenceCompress;
}
bool InvertedIndexConfig::IsReferenceCompress() const { return _impl->isReferenceCompress; }

void InvertedIndexConfig::SetHashTypedDictionary(bool isHashType) { _impl->isHashTypedDictionary = isHashType; }

bool InvertedIndexConfig::IsHashTypedDictionary() const { return _impl->isHashTypedDictionary; }

// Dict
bool InvertedIndexConfig::HasAdaptiveDictionary() const { return _impl->adaptiveDictConfig.operator bool(); }
const std::shared_ptr<indexlib::config::AdaptiveDictionaryConfig>&
InvertedIndexConfig::GetAdaptiveDictionaryConfig() const
{
    return _impl->adaptiveDictConfig;
}
void InvertedIndexConfig::SetAdaptiveDictConfig(
    const std::shared_ptr<indexlib::config::AdaptiveDictionaryConfig>& dictConfig)
{
    _impl->adaptiveDictConfig = dictConfig;
}
void InvertedIndexConfig::SetDictConfig(const std::shared_ptr<indexlib::config::DictionaryConfig>& dictConfig)
{
    const string& nullTermLiteralStr = GetNullTermLiteralString();
    _impl->dictConfig = dictConfig;
    _impl->highFreqVocabulary = HighFreqVocabularyCreator::CreateVocabulary(
        _impl->indexName, _impl->invertedIndexType, dictConfig, nullTermLiteralStr, GetDictHashParams());
}

void InvertedIndexConfig::SetDictConfigWithoutVocabulary(
    const std::shared_ptr<indexlib::config::DictionaryConfig>& dictConfig)
{
    _impl->dictConfig = dictConfig;
}

const std::shared_ptr<indexlib::config::DictionaryConfig>& InvertedIndexConfig::GetDictConfig() const
{
    return _impl->dictConfig;
}
void InvertedIndexConfig::SetHighFreqencyTermPostingType(indexlib::index::HighFrequencyTermPostingType type)
{
    _impl->highFrequencyTermPostingType = type;
}
indexlib::index::HighFrequencyTermPostingType InvertedIndexConfig::GetHighFrequencyTermPostingType() const
{
    return _impl->highFrequencyTermPostingType;
}
void InvertedIndexConfig::SetHighFreqVocabulary(
    const std::shared_ptr<indexlib::config::HighFrequencyVocabulary>& vocabulary)
{
    _impl->highFreqVocabulary = vocabulary;
}
const std::shared_ptr<indexlib::config::HighFrequencyVocabulary>& InvertedIndexConfig::GetHighFreqVocabulary() const
{
    return _impl->highFreqVocabulary;
}

bool InvertedIndexConfig::IsBitmapOnlyTerm(const indexlib::index::DictKeyInfo& key) const
{
    if (!_impl->highFreqVocabulary) {
        return false;
    }

    if (_impl->highFrequencyTermPostingType != indexlib::index::hp_bitmap) {
        return false;
    }

    return _impl->highFreqVocabulary->Lookup(key);
}

void InvertedIndexConfig::SetShardingType(InvertedIndexConfig::IndexShardingType shardingType)
{
    _impl->shardingType = shardingType;
}

InvertedIndexConfig::IndexShardingType InvertedIndexConfig::GetShardingType() const { return _impl->shardingType; }

void InvertedIndexConfig::AppendShardingIndexConfig(const std::shared_ptr<InvertedIndexConfig>& shardingIndexConfig)
{
    assert(_impl->shardingType == InvertedIndexConfig::IST_NEED_SHARDING);
    _impl->shardingIndexConfigs.push_back(shardingIndexConfig);
}

indexid_t InvertedIndexConfig::GetParentIndexId() const { return _impl->parentIndexId; }

void InvertedIndexConfig::SetParentIndexId(indexid_t indexId) { _impl->parentIndexId = indexId; }

const vector<std::shared_ptr<InvertedIndexConfig>>& InvertedIndexConfig::GetShardingIndexConfigs() const
{
    return _impl->shardingIndexConfigs;
}

const char* InvertedIndexConfig::InvertedIndexTypeToStr(InvertedIndexType invertedIndexType)
{
    switch (invertedIndexType) {
    case it_pack:
        return "PACK";
    case it_text:
        return "TEXT";
    case it_expack:
        return "EXPACK";
    case it_string:
        return "STRING";
    case it_enum:
        return "ENUM";
    case it_property:
        return "PROPERTY";
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
        return "NUMBER";
    case it_primarykey64:
        return "PRIMARYKEY64";
    case it_primarykey128:
        return "PRIMARYKEY128";
    case it_trie:
        return "TRIE";
    case it_spatial:
        return "SPATIAL";
    case it_customized:
        return "CUSTOMIZED";
    case it_kv:
    case it_kkv:
        return "PRIMARY_KEY";
    case it_datetime:
        return "DATE"; // TODO: change to DATETIME after online upgrade
    case it_range:
        return "RANGE";
    default:
        return "UNKNOWN";
    }
    return "UNKNOWN";
}

std::pair<Status, InvertedIndexType> InvertedIndexConfig::StrToIndexType(const string& typeStr)
{
    if (!strcasecmp(typeStr.c_str(), "text")) {
        return {Status::OK(), it_text};
    } else if (!strcasecmp(typeStr.c_str(), "string")) {
        return {Status::OK(), it_string};
    } else if (!strcasecmp(typeStr.c_str(), "number")) {
        return {Status::OK(), it_number};
    } else if (!strcasecmp(typeStr.c_str(), "enum")) {
        return {Status::OK(), it_enum};
    } else if (!strcasecmp(typeStr.c_str(), "property")) {
        return {Status::OK(), it_property};
    } else if (!strcasecmp(typeStr.c_str(), "pack")) {
        return {Status::OK(), it_pack};
    } else if (!strcasecmp(typeStr.c_str(), "expack")) {
        return {Status::OK(), it_expack};
    } else if (!strcasecmp(typeStr.c_str(), "primarykey64")) {
        return {Status::OK(), it_primarykey64};
    } else if (!strcasecmp(typeStr.c_str(), "primarykey128")) {
        return {Status::OK(), it_primarykey128};
    } else if (!strcasecmp(typeStr.c_str(), "trie")) {
        return {Status::OK(), it_trie};
    } else if (!strcasecmp(typeStr.c_str(), "spatial")) {
        return {Status::OK(), it_spatial};
    } else if (!strcasecmp(typeStr.c_str(), "date")) {
        return {Status::OK(), it_datetime};
    } else if (!strcasecmp(typeStr.c_str(), "datetime")) {
        return {Status::OK(), it_datetime};
    } else if (!strcasecmp(typeStr.c_str(), "range")) {
        return {Status::OK(), it_range};
    } else if (!strcasecmp(typeStr.c_str(), "customized")) {
        return {Status::OK(), it_customized};
    }

    stringstream ss;
    ss << "Unknown index_type: " << typeStr << ", support index_type are: ";
    for (int it = 0; it < (int)it_unknown; ++it) {
        ss << InvertedIndexTypeToStr((InvertedIndexType)it) << ",";
    }
    AUTIL_LOG(ERROR, "%s", ss.str().c_str());
    return {Status::ConfigError(ss.str().c_str()), it_unknown};
}

string InvertedIndexConfig::GetShardingIndexName(const string& indexName, size_t shardingIdx)
{
    return indexName + "_@_" + autil::StringUtil::toString<size_t>(shardingIdx);
}

bool InvertedIndexConfig::GetIndexNameFromShardingIndexName(const string& shardingIndexName, string& indexName)
{
    size_t pos = shardingIndexName.rfind("_@_");
    if (pos == string::npos) {
        return false;
    }
    string numStr = shardingIndexName.substr(pos + 3);
    uint64_t num = 0;
    if (!StringUtil::fromString(numStr, num)) {
        return false;
    }

    indexName = shardingIndexName.substr(0, pos);
    return true;
}

void InvertedIndexConfig::SetVirtual(bool flag) { _impl->isVirtual = flag; }
bool InvertedIndexConfig::IsVirtual() const { return _impl->isVirtual; }

void InvertedIndexConfig::Disable()
{
    _impl->status = (_impl->status == indexlib::is_normal) ? indexlib::is_disable : _impl->status;
    if (_impl->shardingType == InvertedIndexConfig::IST_NEED_SHARDING) {
        for (size_t i = 0; i < _impl->shardingIndexConfigs.size(); ++i) {
            _impl->shardingIndexConfigs[i]->Disable();
        }
    }
}

void InvertedIndexConfig::Delete() { _impl->status = indexlib::is_deleted; }

bool InvertedIndexConfig::IsDeleted() const { return _impl->status == indexlib::is_deleted; }

bool InvertedIndexConfig::IsNormal() const { return _impl->status == indexlib::is_normal; }

indexlib::IndexStatus InvertedIndexConfig::GetStatus() const { return _impl->status; }

format_versionid_t InvertedIndexConfig::GetIndexFormatVersionId() const { return _impl->formatVersionId; }

format_versionid_t InvertedIndexConfig::GetMaxSupportedIndexFormatVersionId() const
{
    return _impl->maxSupportedFormatVersionId;
}
Status InvertedIndexConfig::SetIndexFormatVersionId(format_versionid_t id)
{
    if (id > InvertedIndexConfig::BINARY_FORMAT_VERSION) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(),
                               "unsupported format version [%d] for index [%s], "
                               "which is bigger than binary supported version [%d]",
                               id, _impl->indexName.c_str(), InvertedIndexConfig::BINARY_FORMAT_VERSION);
    }

    if (id > _impl->maxSupportedFormatVersionId) {
        AUTIL_LOG(INFO, "ignore SetIndexFormatVersionId to [%d] for index [%s], because max supported id is [%d].", id,
                  _impl->indexName.c_str(), _impl->maxSupportedFormatVersionId);
        return Status::OK();
    }
    _impl->formatVersionId = id;
    return Status::OK();
}

Status InvertedIndexConfig::SetMaxSupportedIndexFormatVersionId(format_versionid_t id)
{
    if (id > InvertedIndexConfig::BINARY_FORMAT_VERSION) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(),
                               "unsupported max format version [%d] for index [%s], "
                               "which is bigger than binary supported version [%d]",
                               id, _impl->indexName.c_str(), InvertedIndexConfig::BINARY_FORMAT_VERSION);
    }
    _impl->maxSupportedFormatVersionId = id;
    if (_impl->formatVersionId > _impl->maxSupportedFormatVersionId) {
        AUTIL_LOG(INFO, "reset formatVersionId to [%d] for index [%s].", _impl->maxSupportedFormatVersionId,
                  _impl->indexName.c_str());
        return SetIndexFormatVersionId(_impl->maxSupportedFormatVersionId);
    }
    return Status::OK();
}

bool InvertedIndexConfig::GetBloomFilterParamForDictionary(uint32_t& multipleNum, uint32_t& hashFuncNum) const
{
    if (_impl->bloomFilterMultipleNum == 0 || _impl->bloomFilterMultipleNum == 1) {
        return false;
    }
    multipleNum = _impl->bloomFilterMultipleNum;
    hashFuncNum = GetHashFuncNumForBloomFilter(_impl->bloomFilterMultipleNum);
    return true;
}

uint32_t InvertedIndexConfig::GetHashFuncNumForBloomFilter(uint32_t multipleNum)
{
    const static uint32_t HASH_FUNC_NUM[] = {0, 0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 8, 9, 10, 10, 10};
    if (multipleNum > 16) {
        return 10;
    }
    return HASH_FUNC_NUM[multipleNum];
}

void InvertedIndexConfig::EnableBloomFilterForDictionary(uint32_t multipleNum)
{
    assert(multipleNum <= 16);
    _impl->bloomFilterMultipleNum = multipleNum; // 0 or 1 means disable bloom filter
}

void InvertedIndexConfig::SetOwnerModifyOperationId(indexlib::schema_opid_t opId) { _impl->ownerOpId = opId; }

indexlib::schema_opid_t InvertedIndexConfig::GetOwnerModifyOperationId() const { return _impl->ownerOpId; }

bool InvertedIndexConfig::SupportNull() const
{
    Iterator iter = CreateIterator();
    while (iter.HasNext()) {
        auto fieldConfig = iter.Next();
        assert(fieldConfig);
        if (fieldConfig->IsEnableNullField()) {
            return true;
        }
    }
    return false;
}

string InvertedIndexConfig::GetNullTermLiteralString() const
{
    std::string nullLiteralStr;
    InvertedIndexConfig::Iterator iter = CreateIterator();
    while (iter.HasNext()) {
        auto fieldConfig = iter.Next();
        if (fieldConfig && fieldConfig->IsEnableNullField()) {
            nullLiteralStr = fieldConfig->GetNullFieldLiteralString();
            break;
        }
    }
    return nullLiteralStr;
}

bool InvertedIndexConfig::IsIndexUpdatable() const { return _impl->isIndexUpdatable; }

void InvertedIndexConfig::SetIndexUpdatable(bool updatable) { _impl->isIndexUpdatable = updatable; }

bool InvertedIndexConfig::IsPatchCompressed() const { return _impl->isPatchCompressed; }

void InvertedIndexConfig::SetPatchCompressed(bool compressed) { _impl->isPatchCompressed = compressed; }

void InvertedIndexConfig::SetFileCompressConfig(const std::shared_ptr<FileCompressConfig>& compressConfig)
{
    _impl->fileCompressConfig = compressConfig;
}
void InvertedIndexConfig::SetFileCompressConfigV2(const std::shared_ptr<FileCompressConfigV2>& fileCompressConfigV2)
{
    _impl->fileCompressConfigV2 = fileCompressConfigV2;
}

const std::shared_ptr<FileCompressConfig>& InvertedIndexConfig::GetFileCompressConfig() const
{
    return _impl->fileCompressConfig;
}
const std::shared_ptr<FileCompressConfigV2>& InvertedIndexConfig::GetFileCompressConfigV2() const
{
    return _impl->fileCompressConfigV2;
}

InvertedIndexConfig::Iterator InvertedIndexConfig::CreateIterator() const { return DoCreateIterator(); }

// TODO(makuo.mnb) implement this function when migrate truncate
void InvertedIndexConfig::TEST_SetIndexUpdatable(bool updatable) { SetIndexUpdatable(updatable); }

InvertedIndexType InvertedIndexConfig::FieldTypeToInvertedIndexType(FieldType fieldType)
{
    InvertedIndexType type = it_unknown;
    switch (fieldType) {
    case ft_int8:
        type = it_number_int8;
        break;
    case ft_uint8:
        type = it_number_uint8;
        break;
    case ft_int16:
        type = it_number_int16;
        break;
    case ft_uint16:
        type = it_number_uint16;
        break;
    case ft_integer:
        type = it_number_int32;
        break;
    case ft_uint32:
        type = it_number_uint32;
        break;
    case ft_long:
        type = it_number_int64;
        break;
    case ft_uint64:
        type = it_number_uint64;
        break;
    default:
        assert(false);
        break;
    }
    return type;
}
void InvertedIndexConfig::SetNonTruncateIndexName(const string& indexName) { _impl->nonTruncIndexName = indexName; }
const string& InvertedIndexConfig::GetNonTruncateIndexName() const { return _impl->nonTruncIndexName; }

void InvertedIndexConfig::SetHasTruncateFlag(bool flag) { _impl->hasTruncate = flag; }
bool InvertedIndexConfig::HasTruncate() const { return _impl->hasTruncate; }
void InvertedIndexConfig::SetUseTruncateProfilesStr(const std::string& useTruncateProfiles)
{
    _impl->useTruncateProfiles = useTruncateProfiles;
}

bool InvertedIndexConfig::HasTruncateProfile(const TruncateProfileConfig* truncateProfileConfig) const
{
    if (!_impl->hasTruncate) {
        return false;
    }
    autil::StringTokenizer st(_impl->useTruncateProfiles, USE_TRUNCATE_PROFILES_SEPRATOR,
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
        if (truncateProfileConfig->GetTruncateProfileName() == *it) {
            return true;
        }
    }
    return false;
}

std::string InvertedIndexConfig::GetUseTruncateProfilesStr() const
{
    assert(_impl->hasTruncate);
    return _impl->useTruncateProfiles;
}

void InvertedIndexConfig::SetUseTruncateProfiles(const std::vector<std::string>& profiles)
{
    _impl->useTruncateProfiles = autil::StringUtil::toString(profiles, USE_TRUNCATE_PROFILES_SEPRATOR);
    if (_impl->shardingType == InvertedIndexConfig::IST_NEED_SHARDING) {
        for (size_t i = 0; i < _impl->shardingIndexConfigs.size(); ++i) {
            _impl->shardingIndexConfigs[i]->SetUseTruncateProfiles(profiles);
        }
    }
}

std::vector<std::string> InvertedIndexConfig::GetUseTruncateProfiles() const
{
    autil::StringTokenizer st(_impl->useTruncateProfiles, USE_TRUNCATE_PROFILES_SEPRATOR,
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    return st.getTokenVector();
}

Status
InvertedIndexConfig::LoadTruncateTermVocabulary(const std::shared_ptr<indexlib::file_system::ArchiveFolder>& metaFolder,
                                                const std::vector<std::string>& truncIndexNames)
{
    auto truncTermVocabulary = std::make_shared<TruncateTermVocabulary>(/*legacyArchiveFolder=*/metaFolder);

    auto st = truncTermVocabulary->Init(truncIndexNames);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "init truncate term vocabulary failed.");
        return st;
    }
    if (truncTermVocabulary->GetTermCount() > 0) {
        _impl->truncateTermVocabulary = truncTermVocabulary;
    }
    return Status::OK();
}

Status
InvertedIndexConfig::LoadTruncateTermVocabulary(const std::shared_ptr<indexlib::file_system::IDirectory>& metaFolder,
                                                const std::vector<std::string>& truncIndexNames)
{
    auto truncTermVocabulary = std::make_shared<TruncateTermVocabulary>(/*legacyArchiveFolder=*/nullptr);

    auto st = truncTermVocabulary->Init(metaFolder, truncIndexNames);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "init truncate term vocabulary failed.");
        return st;
    }
    if (truncTermVocabulary->GetTermCount() > 0) {
        _impl->truncateTermVocabulary = truncTermVocabulary;
    }
    return Status::OK();
}

const std::shared_ptr<TruncateTermVocabulary>& InvertedIndexConfig::GetTruncateTermVocabulary() const
{
    return _impl->truncateTermVocabulary;
}

bool InvertedIndexConfig::IsTruncateTerm(const indexlib::index::DictKeyInfo& key) const
{
    if (!_impl->truncateTermVocabulary) {
        return false;
    }
    return _impl->truncateTermVocabulary->Lookup(key);
}

bool InvertedIndexConfig::GetTruncatePostingCount(const indexlib::index::DictKeyInfo& key, int32_t& count) const
{
    count = 0;
    if (!_impl->truncateTermVocabulary) {
        return false;
    }
    return _impl->truncateTermVocabulary->LookupTF(key, count);
}

std::string InvertedIndexConfig::CreateTruncateIndexName(const std::string& indexName,
                                                         const std::string& truncateProfileName)
{
    return indexName + "_" + truncateProfileName;
}

const vector<std::shared_ptr<InvertedIndexConfig>>& InvertedIndexConfig::GetTruncateIndexConfigs() const
{
    return _impl->truncateIndexConfigs;
}

void InvertedIndexConfig::AppendTruncateIndexConfig(const std::shared_ptr<InvertedIndexConfig>& truncateIndexConfig)
{
    bool alreadyExist = false;
    for (size_t i = 0; i < _impl->truncateIndexConfigs.size(); ++i) {
        if (_impl->truncateIndexConfigs[i]->GetIndexName() == truncateIndexConfig->GetIndexName()) {
            alreadyExist = true;
            break;
        }
    }
    if (!alreadyExist) {
        _impl->truncateIndexConfigs.push_back(truncateIndexConfig);
    }
}

void InvertedIndexConfig::SetTruncatePayloadConfig(const indexlib::config::PayloadConfig& payloadConfig)
{
    _impl->payloadConfig = payloadConfig;
}

const indexlib::config::PayloadConfig& InvertedIndexConfig::GetTruncatePayloadConfig() const
{
    return _impl->payloadConfig;
}

Status InvertedIndexConfig::CheckCompatible(const IIndexConfig* other) const
{
    const auto* typedOther = dynamic_cast<const InvertedIndexConfig*>(other);
    if (!typedOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "cast to InvertedIndexConfig failed");
    }
    auto toJsonString = [](const config::IIndexConfig* config) {
        autil::legacy::Jsonizable::JsonWrapper json;
        config->Serialize(json);
        return ToJsonString(json.GetMap());
    };
    const auto& jsonStr = toJsonString(this);
    const auto& jsonStrOther = toJsonString(typedOther);
    if (jsonStr != jsonStrOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "original config [%s] is not compatible with [%s]",
                               jsonStr.c_str(), jsonStrOther.c_str());
    }
    return Status::OK();
}

bool InvertedIndexConfig::IsDisabled() const { return _impl->status == indexlib::is_disable; }

} // namespace indexlibv2::config
