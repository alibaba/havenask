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
#include "indexlib/config/impl/dictionary_schema_impl.h"

#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, DictionarySchemaImpl);

DictionarySchemaImpl::DictionarySchemaImpl() {}

DictionarySchemaImpl::~DictionarySchemaImpl() {}

void DictionarySchemaImpl::AddDictionaryConfig(const std::shared_ptr<DictionaryConfig>& dictConfig)
{
    DictionarySchema::Iterator it = mDictConfigs.find(dictConfig->GetDictName());
    if (it != mDictConfigs.end()) {
        stringstream ss;
        ss << "Duplicated dictionary name:" << dictConfig->GetDictName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    mDictConfigs.insert(make_pair(dictConfig->GetDictName(), dictConfig));
}

std::shared_ptr<DictionaryConfig> DictionarySchemaImpl::GetDictionaryConfig(const string& dictName) const
{
    DictionarySchema::Iterator it = mDictConfigs.find(dictName);
    if (it != mDictConfigs.end()) {
        return it->second;
    }
    return std::shared_ptr<DictionaryConfig>();
}

void DictionarySchemaImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        DictionarySchema::Iterator it = mDictConfigs.begin();
        vector<Any> anyVec;
        anyVec.reserve(mDictConfigs.size());
        for (; it != mDictConfigs.end(); ++it) {
            anyVec.push_back(ToJson(*(it->second)));
        }

        json.Jsonize(DICTIONARIES, anyVec);
    }
}

void DictionarySchemaImpl::AssertEqual(const DictionarySchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mDictConfigs.size(), other.mDictConfigs.size(), "mDictConfigs' size not equal");

    DictionarySchema::Iterator it = mDictConfigs.begin();
    for (; it != mDictConfigs.end(); ++it) {
        std::map<std::string, std::shared_ptr<DictionaryConfig>>::const_iterator iter =
            other.mDictConfigs.find(it->first);
        std::shared_ptr<DictionaryConfig> dictConfig;
        if (iter != other.mDictConfigs.end()) {
            dictConfig = iter->second;
        }

        IE_CONFIG_ASSERT(dictConfig, "dictionary configs do not match");
        auto status = it->second->CheckEqual(*dictConfig);
        THROW_IF_STATUS_ERROR(status);
    }
}

void DictionarySchemaImpl::AssertCompatible(const DictionarySchemaImpl& other) const
{
    IE_CONFIG_ASSERT(mDictConfigs.size() <= other.mDictConfigs.size(), "dictionary schema is not compatible");

    DictionarySchema::Iterator it = mDictConfigs.begin();
    for (; it != mDictConfigs.end(); ++it) {
        std::map<std::string, std::shared_ptr<DictionaryConfig>>::const_iterator iter =
            other.mDictConfigs.find(it->first);
        std::shared_ptr<DictionaryConfig> dictConfig;
        if (iter != other.mDictConfigs.end()) {
            dictConfig = iter->second;
        }
        IE_CONFIG_ASSERT(dictConfig, "dictionary configs do not match");
        auto status = it->second->CheckEqual(*dictConfig);
        THROW_IF_STATUS_ERROR(status);
    }
}
}} // namespace indexlib::config
