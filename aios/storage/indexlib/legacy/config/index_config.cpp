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
#include "indexlib/config/index_config.h"

#include "autil/EnvUtil.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfigSerializer.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace indexlib::util;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, IndexConfig);

struct IndexConfig::Impl {
    vector<IndexConfigPtr> shardingIndexConfigs;
    // customized config
    CustomizedConfigVector customizedConfigs;
    Impl() {}
    Impl(const Impl& other) {}
};

IndexConfig::IndexConfig() : mImpl(make_unique<Impl>()) {}

IndexConfig::IndexConfig(const string& indexName, InvertedIndexType indexType)
    : indexlibv2::config::InvertedIndexConfig(indexName, indexType)
    , mImpl(make_unique<Impl>())
{
}

IndexConfig::IndexConfig(const IndexConfig& other)
    : indexlibv2::config::InvertedIndexConfig(other)
    , mImpl(make_unique<Impl>(*(other.mImpl)))
{
}

IndexConfig::~IndexConfig() {}

void IndexConfig::AssertEqual(const IndexConfig& other) const
{
    auto status = indexlibv2::config::InvertedIndexConfig::CheckEqual(other);
    THROW_IF_STATUS_ERROR(status);
    IE_CONFIG_ASSERT_EQUAL(mImpl->shardingIndexConfigs.size(), other.mImpl->shardingIndexConfigs.size(),
                           "mImpl->shardingIndexConfigs size not equal");
    for (size_t i = 0; i < mImpl->shardingIndexConfigs.size(); i++) {
        mImpl->shardingIndexConfigs[i]->AssertEqual(*other.mImpl->shardingIndexConfigs[i]);
    }
    for (size_t i = 0; i < mImpl->customizedConfigs.size(); i++) {
        mImpl->customizedConfigs[i]->AssertEqual(*other.mImpl->customizedConfigs[i]);
    }
}
void IndexConfig::Check() const { indexlibv2::config::InvertedIndexConfig::Check(); }

bool IndexConfig::FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const
{
    *configV2 = *this;
    auto adaptiveDictConfig = GetAdaptiveDictionaryConfig();
    if (adaptiveDictConfig) {
        configV2->SetAdaptiveDictConfig(std::make_shared<AdaptiveDictionaryConfig>(*adaptiveDictConfig));
    }
    auto dictConfig = GetDictConfig();
    if (dictConfig) {
        auto clonedDictConfig = std::make_shared<DictionaryConfig>(*dictConfig);
        configV2->SetDictConfigWithoutVocabulary(clonedDictConfig);
    }
    auto vocabulary = GetHighFreqVocabulary();
    if (vocabulary) {
        auto clonedVocabulary = std::make_shared<HighFrequencyVocabulary>(*vocabulary);
        configV2->SetHighFreqVocabulary(clonedVocabulary);
    }
    configV2->SetNonTruncateIndexName(GetNonTruncateIndexName());
    for (const auto& shardingConfig : mImpl->shardingIndexConfigs) {
        auto newConfig = shardingConfig->ConstructConfigV2();
        assert(newConfig);
        configV2->AppendShardingIndexConfig(
            std::shared_ptr<indexlibv2::config::InvertedIndexConfig>(newConfig.release()));
    }
    // TODO: truncate configs
    return true;
}

void IndexConfig::SetUseTruncateProfiles(const std::vector<std::string>& profiles)
{
    indexlibv2::config::InvertedIndexConfig::SetUseTruncateProfiles(profiles);
    if (GetShardingType() == IndexConfig::IST_NEED_SHARDING) {
        for (size_t i = 0; i < mImpl->shardingIndexConfigs.size(); ++i) {
            mImpl->shardingIndexConfigs[i]->SetUseTruncateProfiles(profiles);
        }
    }
}

void IndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    map<string, Any> jsonMap = json.GetMap();
    // TODO split to PackIndexConfig and SingleIndexConfig
    if (json.GetMode() == TO_JSON) {
        indexlibv2::config::InvertedIndexConfig::Serialize(json);
        if (mImpl->customizedConfigs.size() > 1) {
            vector<Any> anyVec;
            anyVec.reserve(mImpl->customizedConfigs.size());
            for (size_t i = 0; i < mImpl->customizedConfigs.size(); i++) {
                anyVec.push_back(ToJson(*mImpl->customizedConfigs[i]));
            }
            json.Jsonize(CUSTOMIZED_CONFIG, anyVec);
        }
        if (GetShardingType() == InvertedIndexConfig::IST_NEED_SHARDING) {
            bool needSharding = true;
            json.Jsonize(NEED_SHARDING, needSharding);
        }
        if (GetFileCompressConfig()) {
            string fileCompress = GetFileCompressConfig()->GetCompressName();
            json.Jsonize(FILE_COMPRESS, fileCompress);
        }
    } else {
        indexlib::index::InvertedIndexConfigSerializer::DeserializeCommonFields(jsonMap, this);
        auto iter = jsonMap.find(CUSTOMIZED_CONFIG);
        if (iter != jsonMap.end()) {
            JsonArray customizedConfigVec = AnyCast<JsonArray>(iter->second);
            for (JsonArray::iterator configIter = customizedConfigVec.begin(); configIter != customizedConfigVec.end();
                 ++configIter) {
                JsonMap customizedConfigMap = AnyCast<JsonMap>(*configIter);
                Jsonizable::JsonWrapper jsonWrapper(customizedConfigMap);
                CustomizedConfigPtr customizedConfig(new CustomizedConfig());
                customizedConfig->Jsonize(jsonWrapper);
                mImpl->customizedConfigs.push_back(customizedConfig);
            }
        }
    }
}

void IndexConfig::SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs)
{
    mImpl->customizedConfigs = customizedConfigs;
}

const CustomizedConfigVector& IndexConfig::GetCustomizedConfigs() const { return mImpl->customizedConfigs; }

InvertedIndexType IndexConfig::StrToIndexType(const string& typeStr, TableType tableType)
{
    if (!strcasecmp(typeStr.c_str(), "text")) {
        return it_text;
    } else if (!strcasecmp(typeStr.c_str(), "string")) {
        return it_string;
    } else if (!strcasecmp(typeStr.c_str(), "number")) {
        return it_number;
    } else if (!strcasecmp(typeStr.c_str(), "enum")) {
        return it_enum;
    } else if (!strcasecmp(typeStr.c_str(), "property")) {
        return it_property;
    } else if (!strcasecmp(typeStr.c_str(), "pack")) {
        return it_pack;
    } else if (!strcasecmp(typeStr.c_str(), "expack")) {
        return it_expack;
    } else if (!strcasecmp(typeStr.c_str(), "primarykey64")) {
        return it_primarykey64;
    } else if (!strcasecmp(typeStr.c_str(), "primarykey128")) {
        return it_primarykey128;
    } else if (!strcasecmp(typeStr.c_str(), "trie")) {
        return it_trie;
    } else if (!strcasecmp(typeStr.c_str(), "spatial")) {
        return it_spatial;
    } else if (!strcasecmp(typeStr.c_str(), "date")) {
        return it_datetime;
    } else if (!strcasecmp(typeStr.c_str(), "datetime")) {
        return it_datetime;
    } else if (!strcasecmp(typeStr.c_str(), "range")) {
        return it_range;
    } else if (!strcasecmp(typeStr.c_str(), "customized")) {
        return it_customized;
    } else if (!strcasecmp(typeStr.c_str(), "primary_key")) {
        if (tableType == tt_kv) {
            return it_kv;
        }
        if (tableType == tt_kkv) {
            return it_kkv;
        }
        INDEXLIB_FATAL_ERROR(Schema, "PRIMARY_KEY index type requires KV or KKV type table");
    }

    stringstream ss;
    ss << "Unknown index_type: " << typeStr << ", support index_type are: ";
    for (int it = 0; it < (int)it_unknown; ++it) {
        ss << InvertedIndexTypeToStr((InvertedIndexType)it) << ",";
    }

    INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    return it_unknown;
}

void IndexConfig::AppendShardingIndexConfig(const shared_ptr<InvertedIndexConfig>& shardingIndexConfig)
{
    auto castConfig = dynamic_pointer_cast<IndexConfig>(shardingIndexConfig);
    if (!castConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "sharding index config cast fail, [%s]",
                             shardingIndexConfig->GetIndexName().c_str());
    }
    indexlibv2::config::InvertedIndexConfig::AppendShardingIndexConfig(castConfig);
    mImpl->shardingIndexConfigs.push_back(castConfig);
}
const vector<shared_ptr<IndexConfig>>& IndexConfig::GetShardingIndexConfigs() const
{
    return mImpl->shardingIndexConfigs;
}

indexlibv2::config::InvertedIndexConfig::Iterator IndexConfig::DoCreateIterator() const
{
    vector<shared_ptr<indexlibv2::config::FieldConfig>> fieldConfigs;
    IndexConfig::Iterator iter = CreateIterator();
    while (iter.HasNext()) {
        auto fieldConfig = iter.Next();
        fieldConfigs.emplace_back(fieldConfig);
    }
    return indexlibv2::config::InvertedIndexConfig::Iterator(fieldConfigs);
}

void IndexConfig::DoDeserialize(const autil::legacy::Any& any,
                                const indexlibv2::config::IndexConfigDeserializeResource& resource)
{
}

}} // namespace indexlib::config
