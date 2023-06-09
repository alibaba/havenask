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
#include "indexlib/config/impl/adaptive_dictionary_schema_impl.h"

#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, AdaptiveDictionarySchemaImpl);

AdaptiveDictionarySchemaImpl::AdaptiveDictionarySchemaImpl() {}

AdaptiveDictionarySchemaImpl::~AdaptiveDictionarySchemaImpl() {}

std::shared_ptr<AdaptiveDictionaryConfig>
AdaptiveDictionarySchemaImpl::GetAdaptiveDictionaryConfig(const string& ruleName) const
{
    Iterator it = mDictRules.find(ruleName);
    if (it == mDictRules.end()) {
        return std::shared_ptr<AdaptiveDictionaryConfig>();
    }

    size_t idx = it->second;
    assert(idx < mAdaptiveDictConfigs.size());
    return mAdaptiveDictConfigs[idx];
}

void AdaptiveDictionarySchemaImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize(ADAPTIVE_DICTIONARIES, mAdaptiveDictConfigs, mAdaptiveDictConfigs);
    if (json.GetMode() == Jsonizable::FROM_JSON) {
        mDictRules.clear();
        for (size_t i = 0; i < mAdaptiveDictConfigs.size(); ++i) {
            std::shared_ptr<AdaptiveDictionaryConfig> dictConf = mAdaptiveDictConfigs[i];
            string ruleName = dictConf->GetRuleName();
            Iterator it = mDictRules.find(ruleName);
            if (it != mDictRules.end()) {
                stringstream ss;
                ss << "Duplicated adaptive dictionary name:" << ruleName;
                INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
            }
            mDictRules.insert(make_pair(ruleName, i));
        }
    }
}

void AdaptiveDictionarySchemaImpl::AssertEqual(const AdaptiveDictionarySchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mDictRules.size(), other.mDictRules.size(), "mDictRules' size not equal");

    IE_CONFIG_ASSERT_EQUAL(mAdaptiveDictConfigs.size(), other.mAdaptiveDictConfigs.size(),
                           "mAdaptiveDictConfigs' size not equal");

    Iterator it = mDictRules.begin();
    for (; it != mDictRules.end(); ++it) {
        std::shared_ptr<AdaptiveDictionaryConfig> dictConfig = other.GetAdaptiveDictionaryConfig(it->first);
        IE_CONFIG_ASSERT(dictConfig, "adaptive dictionary configs do not match");
        auto status = mAdaptiveDictConfigs[it->second]->CheckEqual(*dictConfig);
        THROW_IF_STATUS_ERROR(status);
    }
}

void AdaptiveDictionarySchemaImpl::AssertCompatible(const AdaptiveDictionarySchemaImpl& other) const
{
    IE_CONFIG_ASSERT(mDictRules.size() <= other.mDictRules.size(), "adaptive dictionary schema is not compatible");

    IE_CONFIG_ASSERT(mAdaptiveDictConfigs.size() <= other.mAdaptiveDictConfigs.size(),
                     "adaptive dictionary schema is not compatible");

    Iterator it = mDictRules.begin();
    for (; it != mDictRules.end(); ++it) {
        std::shared_ptr<AdaptiveDictionaryConfig> dictConfig = other.GetAdaptiveDictionaryConfig(it->first);
        IE_CONFIG_ASSERT(dictConfig, "adaptive dictionary configs do not match");
        auto status = mAdaptiveDictConfigs[it->second]->CheckEqual(*dictConfig);
        THROW_IF_STATUS_ERROR(status);
    }
}

void AdaptiveDictionarySchemaImpl::AddAdaptiveDictionaryConfig(const std::shared_ptr<AdaptiveDictionaryConfig>& config)
{
    string ruleName = config->GetRuleName();
    Iterator it = mDictRules.find(ruleName);
    if (it != mDictRules.end()) {
        stringstream ss;
        ss << "Duplicated adaptive dictionary name:" << ruleName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    size_t currentDictSize = mAdaptiveDictConfigs.size();
    mDictRules.insert(make_pair(ruleName, currentDictSize));
    mAdaptiveDictConfigs.push_back(config);
}
}} // namespace indexlib::config
