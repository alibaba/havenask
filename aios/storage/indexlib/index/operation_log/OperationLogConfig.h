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

#include <map>
#include <memory>
#include <vector>

#include "indexlib/config/IIndexConfig.h"

namespace indexlibv2::config {
class SingleFieldIndexConfig;
class FieldConfig;
} // namespace indexlibv2::config

namespace indexlib::index {

class OperationLogConfig final : public indexlibv2::config::IIndexConfig
{
public:
    OperationLogConfig(size_t maxBlockSize, bool isCompress) : _maxBlockSize(maxBlockSize), _isCompress(isCompress) {}
    ~OperationLogConfig() = default;

public:
    size_t GetMaxBlockSize() const;
    bool IsCompress() const;
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    const std::string& GetIndexCommonPath() const override;
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> GetFieldConfigs() const override;
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const indexlibv2::config::IndexConfigDeserializeResource& resource) override
    {
    }
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override {}
    void Check() const override { assert(false); }
    Status CheckCompatible(const IIndexConfig* other) const override;
    bool IsDisabled() const override;

public:
    std::shared_ptr<indexlibv2::config::SingleFieldIndexConfig> GetPrimaryKeyIndexConfig() const;

    void AddIndexConfigs(const std::string& indexType, const std::vector<std::shared_ptr<IIndexConfig>>& indexConfigs);
    void SetFieldConfigs(const std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>>& fieldConfigs);

    const std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>&
    GetIndexConfigs(const std::string& indexType) const;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> GetAllFieldConfigs() const;
    bool HasIndexConfig(const std::string& indexType, const std::string& indexName);

private:
    std::map<std::string, std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>> _indexConfigs;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> _allFieldConfigs;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> _usingFieldConfigs;

    size_t _maxBlockSize;
    bool _isCompress;

private:
    friend class OperationIteratorTest;
};

} // namespace indexlib::index
