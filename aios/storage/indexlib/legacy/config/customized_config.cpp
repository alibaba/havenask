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
#include "indexlib/config/customized_config.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "indexlib/config/impl/customized_config_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, CustomizedConfig);

CustomizedConfig::CustomizedConfig() : mImpl(new CustomizedConfigImpl()) {}

CustomizedConfig::CustomizedConfig(const CustomizedConfig& other)
    : mImpl(new CustomizedConfigImpl(*(other.mImpl.get())))
{
}

CustomizedConfig::~CustomizedConfig() {}

void CustomizedConfig::Jsonize(legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

void CustomizedConfig::AssertEqual(const CustomizedConfig& other) const { mImpl->AssertEqual(*(other.mImpl.get())); }

const string& CustomizedConfig::GetPluginName() const { return mImpl->GetPluginName(); }

bool CustomizedConfig::GetParameters(const string& key, string& value) const
{
    return mImpl->GetParameters(key, value);
}

const map<string, string>& CustomizedConfig::GetParameters() const { return mImpl->GetParameters(); }

const string& CustomizedConfig::GetId() const { return mImpl->GetId(); }

void CustomizedConfig::SetId(const string& id) { mImpl->SetId(id); }

void CustomizedConfig::SetPluginName(const string& pluginName) { mImpl->SetPluginName(pluginName); }
}} // namespace indexlib::config
