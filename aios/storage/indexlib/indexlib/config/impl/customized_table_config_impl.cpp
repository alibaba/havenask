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
#include "indexlib/config/impl/customized_table_config_impl.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, CustomizedTableConfigImpl);

CustomizedTableConfigImpl::CustomizedTableConfigImpl() {}

CustomizedTableConfigImpl::CustomizedTableConfigImpl(const CustomizedTableConfigImpl& other)
    : mId("")
    , mPluginName(other.mPluginName)
    , mParameters(other.mParameters)
{
}

CustomizedTableConfigImpl::~CustomizedTableConfigImpl() {}

void CustomizedTableConfigImpl::Jsonize(legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(CUSTOMIZED_PLUGIN_IDENTIFIER, mId, mId);
    if (json.GetMode() == TO_JSON) {
        json.Jsonize(CUSTOMIZED_PLUGIN_NAME, mPluginName);
        json.Jsonize(CUSTOMIZED_PARAMETERS, mParameters);
    } else {
        map<string, Any> data = json.GetMap();
        auto iter = data.find(CUSTOMIZED_PLUGIN_NAME);
        if (iter == data.end()) {
            INDEXLIB_FATAL_ERROR(Schema, "no plugin name is defined");
        }
        mPluginName = AnyCast<string>(iter->second);
        if (mPluginName.empty()) {
            INDEXLIB_FATAL_ERROR(Schema, "plugin name can not be empty");
        }
        iter = data.find(CUSTOMIZED_PARAMETERS);
        if (iter != data.end()) {
            map<string, Any> paramMap = AnyCast<JsonMap>(iter->second);
            map<string, Any>::iterator paramIter = paramMap.begin();
            for (; paramIter != paramMap.end(); paramIter++) {
                string value = AnyCast<string>(paramIter->second);
                mParameters.insert(make_pair(paramIter->first, value));
            }
        }
    }
}

void CustomizedTableConfigImpl::AssertEqual(const CustomizedTableConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mId, other.mId, "mId not equal");
    IE_CONFIG_ASSERT_EQUAL(mPluginName, other.mPluginName, "mPluginName not equal");
    IE_CONFIG_ASSERT_EQUAL(mParameters, other.mParameters, "mParameters not equal");
}

bool CustomizedTableConfigImpl::GetParameters(const string& key, string& value) const
{
    auto iter = mParameters.find(key);
    if (iter == mParameters.end()) {
        return false;
    }
    value = iter->second;
    return true;
}
}} // namespace indexlib::config
