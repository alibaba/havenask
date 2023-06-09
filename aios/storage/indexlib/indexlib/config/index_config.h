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
#ifndef __INDEXLIB_INDEX_CONFIG_H
#define __INDEXLIB_INDEX_CONFIG_H

#include <memory>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Types.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(config, TruncateTermVocabulary);
DECLARE_REFERENCE_CLASS(config, TruncateProfileConfig);
DECLARE_REFERENCE_CLASS(config, PayloadConfig);
DECLARE_REFERENCE_CLASS(config, CustomizedConfig);
DECLARE_REFERENCE_CLASS(config, IndexConfigImpl);

namespace indexlib { namespace config {

typedef std::vector<FieldConfigPtr> FieldConfigVector;
typedef std::vector<CustomizedConfigPtr> CustomizedConfigVector;

class IndexConfig : public autil::legacy::Jsonizable, public indexlibv2::config::InvertedIndexConfig
{
public:
    class Iterator
    {
    public:
        Iterator() {}
        Iterator(const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs) : mFieldConfigs(fieldConfigs), mIdx(0)
        {
        }
        Iterator(const std::shared_ptr<FieldConfig>& fieldConfig) : mIdx(0) { mFieldConfigs.push_back(fieldConfig); }

        ~Iterator() {}

    public:
        bool HasNext() const { return mIdx < mFieldConfigs.size(); }
        std::shared_ptr<FieldConfig> Next()
        {
            if (mIdx < mFieldConfigs.size()) {
                return mFieldConfigs[mIdx++];
            }
            return std::shared_ptr<FieldConfig>();
        }

    private:
        FieldConfigVector mFieldConfigs;
        size_t mIdx;
    };

public:
    IndexConfig();
    IndexConfig(const std::string& indexName, InvertedIndexType indexType);
    IndexConfig(const IndexConfig& other);
    virtual ~IndexConfig();

public:
    virtual void AssertEqual(const IndexConfig& other) const; //= 0;
    virtual void AssertCompatible(const IndexConfig& other) const = 0;
    virtual Iterator CreateIterator() const = 0;
    virtual std::unique_ptr<indexlibv2::config::InvertedIndexConfig> ConstructConfigV2() const = 0;
    IndexConfig* Clone() const override = 0;
    void Check() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    // Truncate
    void SetHasTruncateFlag(bool flag);
    bool HasTruncate() const;
    void SetUseTruncateProfiles(const std::string& useTruncateProfiles);
    bool HasTruncateProfile(const TruncateProfileConfig* truncateProfileConfig) const;
    std::vector<std::string> GetUseTruncateProfiles() const;
    void LoadTruncateTermVocabulary(const file_system::ArchiveFolderPtr& metaFolder,
                                    const std::vector<std::string>& truncIndexNames);
    void LoadTruncateTermVocabulary(const file_system::DirectoryPtr& truncMetaDir,
                                    const std::vector<std::string>& truncIndexNames);
    const TruncateTermVocabularyPtr& GetTruncateTermVocabulary() const;
    bool IsTruncateTerm(const index::DictKeyInfo& key) const override;
    bool GetTruncatePostingCount(const index::DictKeyInfo& key, int32_t& count) const;
    void SetTruncatePayloadConfig(const PayloadConfig& payloadConfig);
    const PayloadConfig& GetTruncatePayloadConfig() const;

    void SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs);
    const CustomizedConfigVector& GetCustomizedConfigs() const;

    void AppendShardingIndexConfig(const std::shared_ptr<InvertedIndexConfig>& shardingIndexConfig) override;
    const std::vector<std::shared_ptr<IndexConfig>>& GetShardingIndexConfigs() const;

public:
    static InvertedIndexType StrToIndexType(const std::string& typeStr, TableType tableType);
    static std::string CreateTruncateIndexName(const std::string& indexName, const std::string& truncateProfileName);

protected:
    InvertedIndexConfig::Iterator DoCreateIterator() const override;
    virtual bool FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const;

    template <typename ConfigV2Type>
    std::unique_ptr<indexlibv2::config::InvertedIndexConfig> DoConstructConfigV2() const
    {
        auto configV2 = std::make_unique<ConfigV2Type>(GetIndexName(), GetInvertedIndexType());
        if (!FulfillConfigV2(configV2.get())) {
            IE_LOG(ERROR, "construct config v2 failed");
            return nullptr;
        }
        return configV2;
    }

private:
    void DoDeserialize(const autil::legacy::Any& any,
                       const indexlibv2::config::IndexConfigDeserializeResource& resource) override final;

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

private:
    friend class SingleFieldIndexConfigTest;
    friend class IndexConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexConfig);
typedef std::vector<IndexConfigPtr> IndexConfigVector;

class IndexConfigIterator
{
public:
    IndexConfigIterator(const IndexConfigVector& indexConfigs) : mConfigs(indexConfigs) {}

    IndexConfigVector::const_iterator Begin() const { return mConfigs.begin(); }

    IndexConfigVector::const_iterator End() const { return mConfigs.end(); }

private:
    IndexConfigVector mConfigs;
};
DEFINE_SHARED_PTR(IndexConfigIterator);
}} // namespace indexlib::config

#endif //__INDEXLIB_INDEX_CONFIG_H
