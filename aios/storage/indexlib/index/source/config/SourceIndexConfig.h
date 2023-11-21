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
#include "indexlib/index/source/Types.h"

namespace indexlib::config {
class ModuleInfo;
class SourceGroupConfig;

} // namespace indexlib::config
namespace indexlibv2::config {
class SourceIndexConfig : public config::IIndexConfig
{
public:
    SourceIndexConfig();
    SourceIndexConfig(const SourceIndexConfig& other);
    ~SourceIndexConfig();

public:
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    const std::string& GetIndexCommonPath() const override;
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<config::FieldConfig>> GetFieldConfigs() const override;
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    void Check() const override;
    Status CheckCompatible(const config::IIndexConfig* other) const override;
    bool IsDisabled() const override;

public:
    index::sourcegroupid_t GetGroupIdByFieldName(const std::string& fieldName) const;
    void DisableAllFields();
    void DisableFieldGroup(index::sourcegroupid_t groupId);

    size_t GetSourceGroupCount() const;
    std::vector<std::shared_ptr<indexlib::config::SourceGroupConfig>> GetGroupConfigs() const;
    void AddGroupConfig(const std::shared_ptr<indexlib::config::SourceGroupConfig>& groupConfig);
    const std::shared_ptr<indexlib::config::SourceGroupConfig>& GetGroupConfig(index::sourcegroupid_t groupId) const;

    std::optional<size_t> GetMergeId() const;
    std::vector<std::shared_ptr<SourceIndexConfig>> CreateSourceIndexConfigForMerge();

private:
    // for test
    void SetFieldConfigs(const std::vector<std::shared_ptr<config::FieldConfig>>& fieldConfigs);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
