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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::file_system {
class Directory;
typedef std::shared_ptr<Directory> DirectoryPtr;
} // namespace indexlib::file_system
namespace indexlib::config {
class FieldConfig;
typedef std::shared_ptr<FieldConfig> FieldConfigPtr;
} // namespace indexlib::config
namespace indexlib::config {
class CustomizedConfig;
typedef std::shared_ptr<CustomizedConfig> CustomizedConfigPtr;
} // namespace indexlib::config
namespace indexlib::config {
class IndexConfigImpl;
typedef std::shared_ptr<IndexConfigImpl> IndexConfigImplPtr;
} // namespace indexlib::config

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
    void SetUseTruncateProfiles(const std::vector<std::string>& profiles) override;
    void SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs);
    const CustomizedConfigVector& GetCustomizedConfigs() const;

    void AppendShardingIndexConfig(const std::shared_ptr<InvertedIndexConfig>& shardingIndexConfig) override;
    const std::vector<std::shared_ptr<IndexConfig>>& GetShardingIndexConfigs() const;

public:
    static InvertedIndexType StrToIndexType(const std::string& typeStr, TableType tableType);

protected:
    InvertedIndexConfig::Iterator DoCreateIterator() const override;
    virtual bool FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const;

    template <typename ConfigV2Type>
    std::unique_ptr<indexlibv2::config::InvertedIndexConfig> DoConstructConfigV2() const
    {
        auto configV2 = std::make_unique<ConfigV2Type>(GetIndexName(), GetInvertedIndexType());
        if (!FulfillConfigV2(configV2.get())) {
            AUTIL_LOG(ERROR, "construct config v2 failed");
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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexConfig> IndexConfigPtr;
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
typedef std::shared_ptr<IndexConfigIterator> IndexConfigIteratorPtr;
}} // namespace indexlib::config
