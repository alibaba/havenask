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
#include "indexlib/config/temperature_layer_config.h"

#include "indexlib/config/impl/temperature_layer_config_impl.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, TemperatureLayerConfig);

TemperatureLayerConfig::TemperatureLayerConfig() : mImpl(new TemperatureLayerConfigImpl()) {}

TemperatureLayerConfig::~TemperatureLayerConfig() {}

void TemperatureLayerConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

void TemperatureLayerConfig::Check(const AttributeSchemaPtr& attributeSchema) const { mImpl->Check(attributeSchema); }

const std::vector<TemperatureConditionPtr>& TemperatureLayerConfig::GetConditions() const
{
    return mImpl->GetConditions();
}

const std::string TemperatureLayerConfig::GetDefaultProperty() const { return mImpl->GetDefaultProperty(); }

void TemperatureLayerConfig::AssertEqual(const TemperatureLayerConfig& other) const
{
    mImpl->AssertEqual(*other.mImpl);
}

bool TemperatureLayerConfig::operator==(const TemperatureLayerConfig& other) const { return *mImpl == *other.mImpl; }

}} // namespace indexlib::config
