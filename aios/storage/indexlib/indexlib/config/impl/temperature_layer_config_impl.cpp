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
#include "indexlib/config/impl/temperature_layer_config_impl.h"

#include "indexlib/config/config_define.h"
#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, TemperatureLayerConfigImpl);

TemperatureLayerConfigImpl::TemperatureLayerConfigImpl() {}

TemperatureLayerConfigImpl::~TemperatureLayerConfigImpl() {}

void TemperatureLayerConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("default_property", mDefaultProperty);
    json.Jsonize("conditions", mConditions, mConditions);
}

void TemperatureLayerConfigImpl::Check(const AttributeSchemaPtr& attributeSchema) const
{
    if (mDefaultProperty != "HOT" && mDefaultProperty != "WARM" && mDefaultProperty != "COLD") {
        INDEXLIB_FATAL_ERROR(Schema, "field Property [%s] can only be Hot,Warm,Cold", mDefaultProperty.c_str());
    }
    map<string, string> field2Fun;
    for (auto& condition : mConditions) {
        condition->Check(attributeSchema, field2Fun);
    }
}

const std::vector<TemperatureConditionPtr>& TemperatureLayerConfigImpl::GetConditions() const { return mConditions; }

const std::string TemperatureLayerConfigImpl::GetDefaultProperty() const { return mDefaultProperty; }

void TemperatureLayerConfigImpl::AssertEqual(const TemperatureLayerConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mDefaultProperty, other.mDefaultProperty, "defaultProperty not equal");
    IE_CONFIG_ASSERT_EQUAL(mConditions.size(), other.mConditions.size(), "condition size not equal");
    for (size_t i = 0; i < mConditions.size(); i++) {
        mConditions[i]->AssertEqual(*(other.mConditions[i]));
    }
}

bool TemperatureLayerConfigImpl::operator==(const TemperatureLayerConfigImpl& other) const
{
    if (mDefaultProperty != other.mDefaultProperty) {
        return false;
    }
    if (mConditions.size() != other.mConditions.size()) {
        return false;
    }
    for (size_t i = 0; i < mConditions.size(); i++) {
        if (*mConditions[i] == *other.mConditions[i]) {
            continue;
        }
        return false;
    }
    return true;
}

}} // namespace indexlib::config
