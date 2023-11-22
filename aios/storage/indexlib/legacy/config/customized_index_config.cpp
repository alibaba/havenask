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
#include "indexlib/config/customized_index_config.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/index/ann/ANNIndexConfig.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, CustomizedIndexConfig);

struct CustomizedIndexConfig::Impl {
    std::string indexerName;
    util::KeyValueMap params;
};

CustomizedIndexConfig::CustomizedIndexConfig(const string& indexName)
    : PackageIndexConfig(indexName, it_customized)
    , mImpl(make_unique<Impl>())
{
    // attention: set property format version id according to function log
    [[maybe_unused]] auto status = SetMaxSupportedIndexFormatVersionId(0);
}

CustomizedIndexConfig::CustomizedIndexConfig(const CustomizedIndexConfig& other)
    : PackageIndexConfig(other)
    , mImpl(make_unique<Impl>(*(other.mImpl)))
{
}

CustomizedIndexConfig::~CustomizedIndexConfig() {}

void CustomizedIndexConfig::Check() const
{
    if (SupportNull()) {
        INDEXLIB_FATAL_ERROR(Schema, "customize index [%s] not support use field which enable null",
                             GetIndexName().c_str());
    }
}

void CustomizedIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    PackageIndexConfig::Jsonize(json);
    json.Jsonize(CUSTOMIZED_INDEXER_NAME, mImpl->indexerName, mImpl->indexerName);
    if (json.GetMode() == TO_JSON) {
        json.Jsonize(CUSTOMIZED_INDEXER_PARAMS, mImpl->params);
    } else {
        map<string, autil::legacy::Any> data = json.GetMap();
        auto iter = data.find(CUSTOMIZED_INDEXER_PARAMS);
        if (iter != data.end()) {
            map<string, autil::legacy::Any> paramMap =
                autil::legacy::AnyCast<autil::legacy::json::JsonMap>(iter->second);
            auto paramIter = paramMap.begin();
            for (; paramIter != paramMap.end(); paramIter++) {
                string value;
                if (paramIter->second.GetType() == typeid(string)) {
                    value = autil::legacy::AnyCast<string>(paramIter->second);
                } else {
                    autil::legacy::json::ToString(paramIter->second, value, true, "");
                }
                mImpl->params.insert(make_pair(paramIter->first, value));
            }
        }
    }
}

void CustomizedIndexConfig::AssertEqual(const IndexConfig& other) const
{
    PackageIndexConfig::AssertEqual(other);
    const CustomizedIndexConfig& typedOther = (const CustomizedIndexConfig&)other;
    IE_CONFIG_ASSERT_EQUAL(mImpl->indexerName, typedOther.mImpl->indexerName, "customized indexer name not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->params, typedOther.mImpl->params, "indexer parameters not equal");
}

IndexConfig* CustomizedIndexConfig::Clone() const { return new CustomizedIndexConfig(*this); }

std::unique_ptr<indexlibv2::config::InvertedIndexConfig> CustomizedIndexConfig::ConstructConfigV2() const
{
    const string& indxerName = mImpl->indexerName;
    if (CustomizedIndexConfig::AITHETA2_INDEXER_NAME == indxerName ||
        CustomizedIndexConfig::AITHETA_INDEXER_NAME == indxerName) {
        return DoConstructConfigV2<indexlibv2::config::ANNIndexConfig>();
    } else {
        // assert(false);
        AUTIL_LOG(ERROR, "not support construct configv2 for indexerName[%s].", indxerName.c_str());
        return nullptr;
    }
}

bool CustomizedIndexConfig::FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const
{
    if (!PackageIndexConfig::FulfillConfigV2(configV2)) {
        AUTIL_LOG(ERROR, "fulfill package index config failed");
        return false;
    }
    const string& indxerName = mImpl->indexerName;
    if (CustomizedIndexConfig::AITHETA2_INDEXER_NAME == indxerName ||
        CustomizedIndexConfig::AITHETA_INDEXER_NAME == indxerName) {
        auto annConfigV2 = dynamic_cast<indexlibv2::config::ANNIndexConfig*>(configV2);
        assert(annConfigV2);
        annConfigV2->SetIndexer(mImpl->indexerName);
        annConfigV2->SetParameters(mImpl->params);
    } else {
        AUTIL_LOG(ERROR, "not support fill configv2 for indexerName[%s].", indxerName.c_str());
        return false;
    }
    return true;
}

void CustomizedIndexConfig::AssertCompatible(const IndexConfig& other) const { AssertEqual(other); }

const std::string& CustomizedIndexConfig::GetIndexerName() const { return mImpl->indexerName; }
const util::KeyValueMap& CustomizedIndexConfig::GetParameters() const { return mImpl->params; }
void CustomizedIndexConfig::SetIndexer(const std::string& indexerName) { mImpl->indexerName = indexerName; }
void CustomizedIndexConfig::SetParameters(const util::KeyValueMap& params) { mImpl->params = params; }
bool CustomizedIndexConfig::CheckFieldType(FieldType ft) const
{
    // support all field type
    return true;
}
}} // namespace indexlib::config
