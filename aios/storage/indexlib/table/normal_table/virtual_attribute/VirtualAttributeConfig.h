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
#pragma once

#include "autil/Log.h"
#include "indexlib/config/IIndexConfig.h"

namespace indexlibv2 { namespace config {
class AttributeConfig;
}} // namespace indexlibv2::config

namespace indexlibv2::table {

class VirtualAttributeConfig : public config::IIndexConfig
{
public:
    VirtualAttributeConfig(const std::shared_ptr<indexlibv2::config::AttributeConfig>& attrConfig);
    ~VirtualAttributeConfig();

    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    std::vector<std::string> GetIndexPath() const override;
    void Check() const override;
    std::shared_ptr<config::IIndexConfig> GetAttributeConfig() const;
    void Deserialize(const autil::legacy::Any&, size_t idxInJsonArray,
                     const indexlibv2::config::IndexConfigDeserializeResource&) override
    {
    }
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override {}
    std::vector<std::shared_ptr<config::FieldConfig>> GetFieldConfigs() const override;
    Status CheckCompatible(const IIndexConfig* other) const override;

private:
    std::shared_ptr<indexlibv2::config::AttributeConfig> _attrConfig;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
