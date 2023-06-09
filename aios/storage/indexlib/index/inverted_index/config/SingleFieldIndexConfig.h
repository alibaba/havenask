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
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlibv2::config {

class SingleFieldIndexConfig : public InvertedIndexConfig
{
public:
    SingleFieldIndexConfig(const std::string& indexName, InvertedIndexType indexType);
    SingleFieldIndexConfig(const SingleFieldIndexConfig& other);
    ~SingleFieldIndexConfig();
    SingleFieldIndexConfig& operator=(const SingleFieldIndexConfig& other);

public:
    uint32_t GetFieldCount() const override;
    void Check() const override;
    bool IsInIndex(fieldid_t fieldId) const override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    Status CheckEqual(const InvertedIndexConfig& other) const override;
    InvertedIndexConfig* Clone() const override;

    int32_t GetFieldIdxInPack(fieldid_t id) const override;
    std::map<std::string, std::string> GetDictHashParams() const override;

protected:
    bool CheckFieldType(FieldType ft) const override;
    InvertedIndexConfig::Iterator DoCreateIterator() const override;
    void DoDeserialize(const autil::legacy::Any& any, const config::IndexConfigDeserializeResource& resource) override;

public:
    Status SetFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig);
    std::shared_ptr<FieldConfig> GetFieldConfig() const;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override;

private:
    void CheckWhetherIsVirtualField() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    friend class SingleFieldIndexConfigTest;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
