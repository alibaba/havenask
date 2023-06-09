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
#include "indexlib/config/value_config.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/impl/value_config_impl.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, ValueConfig);

ValueConfig::ValueConfig() : mImpl(new ValueConfigImpl) {}

ValueConfig::ValueConfig(const ValueConfig& other)
    : indexlibv2::config::ValueConfig(other)
    , mImpl(new ValueConfigImpl(*other.mImpl))
{
}

ValueConfig::~ValueConfig() {}

void ValueConfig::Init(const vector<AttributeConfigPtr>& attrConfigs)
{
    vector<std::shared_ptr<indexlibv2::config::AttributeConfig>> attrConfigsV2;
    attrConfigsV2.reserve(attrConfigs.size());
    for (const auto& attrConfig : attrConfigs) {
        attrConfigsV2.emplace_back(attrConfig);
    }
    auto status = indexlibv2::config::ValueConfig::Init(attrConfigsV2);
    THROW_IF_STATUS_ERROR(status);
    mImpl->Init(attrConfigs);
}

const AttributeConfigPtr& ValueConfig::GetAttributeConfig(size_t idx) const { return mImpl->GetAttributeConfig(idx); }

config::PackAttributeConfigPtr ValueConfig::CreatePackAttributeConfig() const
{
    CompressTypeOption compressOption;
    THROW_IF_STATUS_ERROR(compressOption.Init(""));
    PackAttributeConfigPtr packAttrConfig(new PackAttributeConfig("pack_values", compressOption,
                                                                  index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT,
                                                                  std::shared_ptr<FileCompressConfig>()));

    for (size_t i = 0; i < GetAttributeCount(); ++i) {
        auto status = packAttrConfig->AddAttributeConfig(GetAttributeConfig(i));
        THROW_IF_STATUS_ERROR(status);
    }

    if (IsValueImpact()) {
        packAttrConfig->EnableImpact();
    }
    if (IsPlainFormat()) {
        packAttrConfig->EnablePlainFormat();
    }
    return packAttrConfig;
}

}} // namespace indexlib::config
