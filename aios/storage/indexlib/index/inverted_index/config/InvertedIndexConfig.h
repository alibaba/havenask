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

#include <memory>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib::index {
class DictKeyInfo;
class InvertedIndexConfigSerializer;
} // namespace indexlib::index

namespace indexlib::file_system {
class IDirectory;
class ArchiveFolder;
} // namespace indexlib::file_system

namespace indexlib::config {
class AdaptiveDictionaryConfig;
class DictionaryConfig;
class HighFrequencyVocabulary;
class FileCompressConfig;
class PayloadConfig;
} // namespace indexlib::config

namespace indexlibv2::config {
class FieldConfig;
class FileCompressConfigV2;
class TruncateTermVocabulary;
class TruncateProfileConfig;

class InvertedIndexConfig : public indexlibv2::config::IIndexConfig
{
public:
    /*
     *   1. IndexWriter will write specific index format according to formatVersionId in InvertedIndexConfig
     *   2. InvertedIndexReader need support read all index format indicated by BINARY_FORMAT_VERSION
     *   3. InvertedIndexConfig with format version id larger than BINARY_FORMAT_VERSION will throw exception
     *   4. SHOULD NOT UPDATE offline binary in single generation with different DEFAULT_FORMAT_VERSION
     *      because Offline config may be confused between different binary version
     *   Attention:
     *        DEFAULT_FORMAT_VERSION <= BINARY_FORMAT_VERSION
     * ==============================================================================================
     * Function log for Format version:
     * * BINARY_FORMAT_VERSION = 0
     *       # base line function
     * ----------------------------------------------------------------------------------------------
     * * BINARY_FORMAT_VERSION = 1
     *       # enable doclist dict inline
     *       # enable short list vbyte compress for doclist by default (could be disabled)
     *       # p4delta compress will optimize equal buffer
     * ----------------------------------------------------------------------------------------------
     */
    static const format_versionid_t BINARY_FORMAT_VERSION; // 1
    static format_versionid_t DEFAULT_FORMAT_VERSION;      // 0

    // IST_NEED_SHARDING is the outer config type, IST_IS_SHARDING is the inner config type.
    // if config->GetShardingType()==IST_NEED_SHARDING,  calling config->GetShardingIndexConfigs() returns
    // vector of configs of type IST_IS_SHARDING.
    // config(IST_NEED_SHARDING)
    // ---sub config 1(IST_IS_SHARDING)
    // ---sub config 2(IST_IS_SHARDING)
    // ...
    enum IndexShardingType {
        IST_NO_SHARDING,
        IST_NEED_SHARDING,
        IST_IS_SHARDING,
    };

public:
    class Iterator
    {
    public:
        Iterator();
        Iterator(const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs);
        Iterator(const std::shared_ptr<FieldConfig>& fieldConfig);
        ~Iterator();

    public:
        bool HasNext() const;
        std::shared_ptr<FieldConfig> Next();

    private:
        std::vector<std::shared_ptr<FieldConfig>> _fieldConfigs;
        size_t _idx = 0;
    };

public:
    InvertedIndexConfig();
    InvertedIndexConfig(const std::string& indexName, InvertedIndexType invertedIndexType);
    InvertedIndexConfig(const InvertedIndexConfig& other);
    ~InvertedIndexConfig();
    InvertedIndexConfig& operator=(const InvertedIndexConfig& other);

public:
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    const std::string& GetIndexCommonPath() const override final;
    std::vector<std::string> GetIndexPath() const override;
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override final;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    void Check() const override;
    Status CheckCompatible(const IIndexConfig* other) const override;
    bool IsDisabled() const override;

public:
    virtual InvertedIndexConfig* Clone() const = 0;
    virtual uint32_t GetFieldCount() const = 0;
    virtual int32_t GetFieldIdxInPack(fieldid_t id) const;
    virtual bool IsInIndex(fieldid_t fieldId) const = 0;
    virtual Status CheckEqual(const InvertedIndexConfig& other) const; //= 0;
    virtual bool CheckFieldType(FieldType ft) const = 0;
    virtual std::map<std::string, std::string> GetDictHashParams() const = 0;
    virtual void SetFileCompressConfig(const std::shared_ptr<indexlib::config::FileCompressConfig>& compressConfig);
    virtual void SetFileCompressConfigV2(const std::shared_ptr<FileCompressConfigV2>& fileCompressConfigV2);
    virtual void AppendShardingIndexConfig(const std::shared_ptr<InvertedIndexConfig>& shardingIndexConfig);
    virtual void AppendTruncateIndexConfig(const std::shared_ptr<InvertedIndexConfig>& truncateIndexConfig);

public:
    // Returns both IST_IS_SHARDING and IST_NEED_SHARDING configs.
    Iterator CreateIterator() const;

    const std::shared_ptr<indexlib::config::FileCompressConfig>& GetFileCompressConfig() const;
    const std::shared_ptr<FileCompressConfigV2>& GetFileCompressConfigV2() const;

    format_versionid_t GetIndexFormatVersionId() const;
    Status SetIndexFormatVersionId(format_versionid_t id);
    Status SetMaxSupportedIndexFormatVersionId(format_versionid_t id);

    indexid_t GetIndexId() const;
    void SetIndexId(indexid_t id);

    indexid_t GetParentIndexId() const;
    void SetParentIndexId(indexid_t indexId);

    void SetIndexName(const std::string& indexName);

    InvertedIndexType GetInvertedIndexType() const;
    void SetInvertedIndexType(InvertedIndexType invertedIndexType);

    const std::string& GetAnalyzer() const;
    void SetAnalyzer(const std::string& analyzerName);

    void SetOptionFlag(optionflag_t optionFlag);
    optionflag_t GetOptionFlag() const;

    void SetVirtual(bool flag);
    bool IsVirtual() const;

    void Disable();
    void Delete();
    bool IsDeleted() const;
    bool IsNormal() const;
    indexlib::IndexStatus GetStatus() const;

    bool IsShortListVbyteCompress() const;
    void SetShortListVbyteCompress(bool isShortListVbyteCompress);

    void SetIsReferenceCompress(bool isReferenceCompress);
    bool IsReferenceCompress() const;

    void SetHashTypedDictionary(bool isHashType);
    bool IsHashTypedDictionary() const;

    // truncate
    virtual bool IsTruncateTerm(const indexlib::index::DictKeyInfo& key) const;
    // deprecated
    void SetNonTruncateIndexName(const std::string& indexName);
    const std::string& GetNonTruncateIndexName() const;

    void SetHasTruncateFlag(bool flag);
    bool HasTruncate() const;
    void SetUseTruncateProfilesStr(const std::string& useTruncateProfiles);

    std::string GetUseTruncateProfilesStr() const;
    bool HasTruncateProfile(const TruncateProfileConfig* truncateProfileConfig) const;
    virtual void SetUseTruncateProfiles(const std::vector<std::string>& profiles);
    std::vector<std::string> GetUseTruncateProfiles() const;
    Status LoadTruncateTermVocabulary(const std::shared_ptr<indexlib::file_system::ArchiveFolder>& metaFolder,
                                      const std::vector<std::string>& truncIndexNames);
    Status LoadTruncateTermVocabulary(const std::shared_ptr<indexlib::file_system::IDirectory>& metaFolder,
                                      const std::vector<std::string>& truncIndexNames);
    const std::shared_ptr<TruncateTermVocabulary>& GetTruncateTermVocabulary() const;
    bool GetTruncatePostingCount(const indexlib::index::DictKeyInfo& key, int32_t& count) const;
    const std::vector<std::shared_ptr<InvertedIndexConfig>>& GetTruncateIndexConfigs() const;
    void SetTruncatePayloadConfig(const indexlib::config::PayloadConfig& payloadConfig);
    const indexlib::config::PayloadConfig& GetTruncatePayloadConfig() const;

    // Dict
    bool HasAdaptiveDictionary() const;
    const std::shared_ptr<indexlib::config::AdaptiveDictionaryConfig>& GetAdaptiveDictionaryConfig() const;
    void SetAdaptiveDictConfig(const std::shared_ptr<indexlib::config::AdaptiveDictionaryConfig>& dictConfig);
    void SetDictConfig(const std::shared_ptr<indexlib::config::DictionaryConfig>& dictConfig);
    void SetDictConfigWithoutVocabulary(const std::shared_ptr<indexlib::config::DictionaryConfig>& dictConfig);
    const std::shared_ptr<indexlib::config::DictionaryConfig>& GetDictConfig() const;
    void SetHighFreqencyTermPostingType(indexlib::index::HighFrequencyTermPostingType type);
    indexlib::index::HighFrequencyTermPostingType GetHighFrequencyTermPostingType() const;
    void SetHighFreqVocabulary(const std::shared_ptr<indexlib::config::HighFrequencyVocabulary>& vocabulary);
    const std::shared_ptr<indexlib::config::HighFrequencyVocabulary>& GetHighFreqVocabulary() const;
    bool IsBitmapOnlyTerm(const indexlib::index::DictKeyInfo& key) const;
    void SetShardingType(IndexShardingType shardingType);
    IndexShardingType GetShardingType() const;
    const std::vector<std::shared_ptr<InvertedIndexConfig>>& GetShardingIndexConfigs() const;

    void SetOwnerModifyOperationId(indexlib::schema_opid_t opId);
    indexlib::schema_opid_t GetOwnerModifyOperationId() const;
    bool SupportNull() const;
    std::string GetNullTermLiteralString() const;

    bool IsIndexUpdatable() const;
    bool IsPatchCompressed() const;

    bool GetBloomFilterParamForDictionary(uint32_t& multipleNum, uint32_t& hashFuncNum) const;
    void EnableBloomFilterForDictionary(uint32_t multipleNum);

public:
    static const char* InvertedIndexTypeToStr(InvertedIndexType invertedIndexType);
    static std::pair<Status, InvertedIndexType> StrToIndexType(const std::string& typeStr);
    static InvertedIndexType FieldTypeToInvertedIndexType(FieldType fieldType);

    static std::string CreateTruncateIndexName(const std::string& indexName, const std::string& truncateProfileName);
    static std::string GetShardingIndexName(const std::string& indexName, size_t shardingIdx);
    static bool GetIndexNameFromShardingIndexName(const std::string& shardingIndexName, std::string& indexName);
    static void ResetDefaultFormatVersion();
    static uint32_t GetHashFuncNumForBloomFilter(uint32_t multipleNum);
    template <typename Schema>
    static std::shared_ptr<InvertedIndexConfig> GetShardingIndexConfig(const Schema& schema,
                                                                       const std::string& shardingIndexName);

public:
    void TEST_SetIndexUpdatable(bool updatable);

protected:
    virtual Iterator DoCreateIterator() const = 0;
    virtual void DoDeserialize(const autil::legacy::Any& any,
                               const config::IndexConfigDeserializeResource& resource) = 0;

private:
    void SetIndexUpdatable(bool updatable);
    void SetPatchCompressed(bool compressed);

protected:
    inline static const std::string INDEX_NAME = "index_name";
    inline static const std::string INDEX_ANALYZER = "index_analyzer";
    inline static const std::string INDEX_TYPE = "index_type";
    inline static const std::string INDEX_FORMAT_VERSIONID = "format_version_id";
    inline static const std::string NEED_SHARDING = "need_sharding";
    inline static const std::string SHARDING_COUNT = "sharding_count";
    inline static const std::string TERM_PAYLOAD_FLAG = "term_payload_flag";
    inline static const std::string DOC_PAYLOAD_FLAG = "doc_payload_flag";
    inline static const std::string POSITION_PAYLOAD_FLAG = "position_payload_flag";
    inline static const std::string POSITION_LIST_FLAG = "position_list_flag";
    inline static const std::string TERM_FREQUENCY_FLAG = "term_frequency_flag";
    inline static const std::string TERM_FREQUENCY_BITMAP = "term_frequency_bitmap";
    inline static const std::string HIGH_FEQUENCY_DICTIONARY = "high_frequency_dictionary";
    inline static const std::string HIGH_FEQUENCY_ADAPTIVE_DICTIONARY = "high_frequency_adaptive_dictionary";
    inline static const std::string HIGH_FEQUENCY_TERM_POSTING_TYPE = "high_frequency_term_posting_type";
    inline static const std::string HAS_SECTION_WEIGHT = "has_section_weight";
    inline static const std::string HAS_SECTION_FIELD_ID = "has_field_id";
    inline static const std::string HIGH_FREQUENCY_DICTIONARY_SELF_ADAPTIVE_FLAG =
        "high_frequency_dictionary_self_adaptive_flag";
    inline static const std::string HAS_SHORTLIST_VBYTE_COMPRESS = "has_shortlist_vbyte_compress";
    inline static const std::string INDEX_COMPRESS_MODE = "compress_mode";
    inline static const std::string INDEX_COMPRESS_MODE_PFOR_DELTA = "pfordelta";
    inline static const std::string INDEX_COMPRESS_MODE_REFERENCE = "reference";
    inline static const std::string USE_HASH_DICTIONARY = "use_hash_typed_dictionary";
    inline static const std::string INDEX_UPDATABLE = "index_updatable";
    inline static const std::string PATCH_COMPRESSED = "patch_compressed";
    inline static const std::string HIGH_FREQ_TERM_BOTH_POSTING = "both";
    inline static const std::string HIGH_FREQ_TERM_BITMAP_POSTING = "bitmap";
    inline static const std::string INDEX_FIELDS = "index_fields";
    inline static const std::string DICTIONARIES = "dictionaries";
    inline static const std::string ADAPTIVE_DICTIONARIES = "adaptive_dictionaries";
    inline static const std::string HAS_TRUNCATE = "has_truncate";
    inline static const std::string USE_TRUNCATE_PROFILES = "use_truncate_profiles";
    inline static const std::string USE_TRUNCATE_PROFILES_SEPRATOR = ";";

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    friend class indexlib::index::InvertedIndexConfigSerializer;
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////////////////
template <typename Schema>
inline std::shared_ptr<InvertedIndexConfig>
InvertedIndexConfig::GetShardingIndexConfig(const Schema& schema, const std::string& shardingIndexName)
{
    std::string originalIndexName;
    if (!GetIndexNameFromShardingIndexName(shardingIndexName, originalIndexName)) {
        return nullptr;
    }
    auto originalIndexConfig = schema.GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, originalIndexName);
    if (!originalIndexConfig) {
        return nullptr;
    }
    auto typedIndexConfig = dynamic_cast<InvertedIndexConfig*>(originalIndexConfig.get());
    assert(typedIndexConfig);
    const auto& indexConfigs = typedIndexConfig->GetShardingIndexConfigs();
    for (const auto& shardingConfig : indexConfigs) {
        if (shardingConfig->GetIndexName() == shardingIndexName) {
            return shardingConfig;
        }
    }
    return nullptr;
}

} // namespace indexlibv2::config
