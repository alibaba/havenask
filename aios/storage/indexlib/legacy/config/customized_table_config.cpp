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
#include "indexlib/config/customized_table_config.h"

#include "indexlib/config/impl/customized_table_config_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, CustomizedTableConfig);

CustomizedTableConfig::CustomizedTableConfig() : mImpl(new CustomizedTableConfigImpl) {}

CustomizedTableConfig::CustomizedTableConfig(const CustomizedTableConfig& other)
    : mImpl(new CustomizedTableConfigImpl(*(other.mImpl.get())))
{
}

CustomizedTableConfig::~CustomizedTableConfig() {}

void CustomizedTableConfig::Jsonize(legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

void CustomizedTableConfig::AssertEqual(const CustomizedTableConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

const string& CustomizedTableConfig::GetIdentifier() const { return mImpl->GetIdentifier(); }

const string& CustomizedTableConfig::GetPluginName() const { return mImpl->GetPluginName(); }

bool CustomizedTableConfig::GetParameters(const string& key, string& value) const
{
    return mImpl->GetParameters(key, value);
}

const map<string, string>& CustomizedTableConfig::GetParameters() const { return mImpl->GetParameters(); }
}} // namespace indexlib::config
