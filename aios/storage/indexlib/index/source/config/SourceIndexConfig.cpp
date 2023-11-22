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
#include "indexlib/index/source/config/SourceIndexConfig.h"

#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/module_info.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/Constant.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "indexlib/util/Algorithm.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, SourceIndexConfig);

struct SourceIndexConfig::Impl {
    std::vector<std::shared_ptr<indexlib::config::SourceGroupConfig>> groupConfigs;
    indexlib::config::ModuleInfos modules;
    std::vector<std::shared_ptr<config::FieldConfig>> fieldConfigs;

    // not jsonize
    std::optional<size_t> mergeId = std::nullopt;
};

SourceIndexConfig::SourceIndexConfig(const SourceIndexConfig& other)
{
    _impl = std::make_unique<Impl>();
    *_impl = *(other._impl);
}

SourceIndexConfig::SourceIndexConfig() : _impl(std::make_unique<Impl>()) {}

SourceIndexConfig::~SourceIndexConfig() {}

const std::string& SourceIndexConfig::GetIndexType() const { return index::SOURCE_INDEX_TYPE_STR; }
const std::string& SourceIndexConfig::GetIndexName() const { return index::SOURCE_INDEX_NAME; }
const std::string& SourceIndexConfig::GetIndexCommonPath() const { return index::SOURCE_INDEX_PATH; }
std::vector<std::string> SourceIndexConfig::GetIndexPath() const { return {index::SOURCE_INDEX_PATH}; }
std::vector<std::shared_ptr<FieldConfig>> SourceIndexConfig::GetFieldConfigs() const { return _impl->fieldConfigs; }
void SourceIndexConfig::Deserialize(const autil::legacy::Any& any, size_t,
                                    const IndexConfigDeserializeResource& resource)
{
    autil::legacy::Jsonizable::JsonWrapper jsonWrapper(any);
    jsonWrapper.Jsonize("modules", _impl->modules, _impl->modules);
    jsonWrapper.Jsonize("group_configs", _impl->groupConfigs, _impl->groupConfigs);
    for (size_t i = 0; i < _impl->groupConfigs.size(); ++i) {
        _impl->groupConfigs[i]->SetGroupId(i);
    }

    std::vector<std::string> fieldNames;
    for (auto& sourceGroupConfig : _impl->groupConfigs) {
        auto compressName = sourceGroupConfig->GetParameter().GetFileCompressName();
        if (!compressName.empty()) {
            auto newParam = sourceGroupConfig->GetParameter();
            auto fileCompressConfig = resource.GetFileCompressConfig(compressName);
            if (!fileCompressConfig) {
                AUTIL_LOG(ERROR, "get file compress [%s] from setting failed", compressName.c_str());
                INDEXLIB_FATAL_ERROR(Schema, "get file compress[%s] failed", compressName.c_str());
            }
            newParam.SetFileCompressConfigV2(fileCompressConfig);
            sourceGroupConfig->SetParameter(newParam);
        }
        if (sourceGroupConfig->GetFieldMode() == indexlib::config::SourceGroupConfig::SPECIFIED_FIELD) {
            const auto& specifiedFields = sourceGroupConfig->GetSpecifiedFields();
            fieldNames.insert(fieldNames.end(), specifiedFields.begin(), specifiedFields.end());
        } else {
            auto allFieldConfigs = resource.GetAllFieldConfigs();
            for (const auto& fieldConfig : allFieldConfigs) {
                fieldNames.push_back(fieldConfig->GetFieldName());
            }
        }
    }
    indexlib::util::Algorithm::SortUniqueAndErase(fieldNames);
    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    for (auto fieldName : fieldNames) {
        if (auto fieldConfig = resource.GetFieldConfig(fieldName)) {
            fieldConfigs.push_back(fieldConfig);
        }
    }
    _impl->fieldConfigs = fieldConfigs;
}
void SourceIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    json.Jsonize("group_configs", _impl->groupConfigs);
}
void SourceIndexConfig::Check() const
{
    if (_impl->groupConfigs.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "source index should have at least one group config.");
    }

    for (size_t i = 0; i < _impl->groupConfigs.size(); ++i) {
        auto groupConfig = _impl->groupConfigs[i];
        auto status = groupConfig->Check();
        if (!status.IsOK()) {
            INDEXLIB_FATAL_ERROR(Schema, "SourceGroupConfig[%zu] check failed", i);
        }

        if (groupConfig->GetFieldMode() == indexlib::config::SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD) {
            for (auto fieldName : groupConfig->GetSpecifiedFields()) {
                auto iter = std::find_if(
                    _impl->fieldConfigs.begin(), _impl->fieldConfigs.end(),
                    [&fieldName](const auto& fieldConfig) { return fieldConfig->GetFieldName() == fieldName; });
                if (iter == _impl->fieldConfigs.end()) {
                    INDEXLIB_FATAL_ERROR(Schema,
                                         "source field [%s] in group[%u]"
                                         "is not defined in field shema",
                                         fieldName.c_str(), groupConfig->GetGroupId());
                }
            }
        }
    }
}

// check compatible when alter table
Status SourceIndexConfig::CheckCompatible(const IIndexConfig* other) const
{
    assert(false);
    return Status::Unimplement();
}

size_t SourceIndexConfig::GetSourceGroupCount() const { return _impl->groupConfigs.size(); }

std::vector<std::shared_ptr<indexlib::config::SourceGroupConfig>> SourceIndexConfig::GetGroupConfigs() const
{
    return _impl->groupConfigs;
}

const std::shared_ptr<indexlib::config::SourceGroupConfig>&
SourceIndexConfig::GetGroupConfig(index::sourcegroupid_t groupId) const
{
    if (groupId < 0 || groupId >= (index::sourcegroupid_t)_impl->groupConfigs.size()) {
        static std::shared_ptr<indexlib::config::SourceGroupConfig> emptyConfig;
        return emptyConfig;
    }
    return _impl->groupConfigs[groupId];
}

void SourceIndexConfig::AddGroupConfig(const std::shared_ptr<indexlib::config::SourceGroupConfig>& groupConfig)
{
    _impl->groupConfigs.push_back(groupConfig);
}

std::optional<size_t> SourceIndexConfig::GetMergeId() const { return _impl->mergeId; }
std::vector<std::shared_ptr<SourceIndexConfig>> SourceIndexConfig::CreateSourceIndexConfigForMerge()
{
    std::vector<std::shared_ptr<SourceIndexConfig>> configs;
    for (size_t i = 0; i < GetSourceGroupCount(); ++i) {
        std::shared_ptr<SourceIndexConfig> singleConfig(new SourceIndexConfig(*this));
        singleConfig->_impl->mergeId = i;
        configs.push_back(singleConfig);
    }
    std::shared_ptr<SourceIndexConfig> metaConfig(new SourceIndexConfig(*this));
    metaConfig->_impl->mergeId = GetSourceGroupCount();
    configs.push_back(metaConfig);
    return configs;
}

bool SourceIndexConfig::IsDisabled() const
{
    for (const auto& groupConfig : _impl->groupConfigs) {
        if (!groupConfig->IsDisabled()) {
            return false;
        }
    }
    return true;
}

void SourceIndexConfig::DisableAllFields()
{
    AUTIL_LOG(INFO, "disable all source group");
    for (const auto& groupConfig : _impl->groupConfigs) {
        groupConfig->SetDisabled(true);
    }
}
void SourceIndexConfig::DisableFieldGroup(index::sourcegroupid_t groupId)
{
    if (groupId < _impl->groupConfigs.size()) {
        AUTIL_LOG(INFO, "disable group [%d] for source success", groupId);
        _impl->groupConfigs[groupId]->SetDisabled(true);
    } else {
        AUTIL_LOG(WARN, "disable group [%d] for source failed, total group count is [%lu]", groupId,
                  GetSourceGroupCount());
    }
}

void SourceIndexConfig::SetFieldConfigs(const std::vector<std::shared_ptr<config::FieldConfig>>& fieldConfigs)
{
    _impl->fieldConfigs = fieldConfigs;
}

index::sourcegroupid_t SourceIndexConfig::GetGroupIdByFieldName(const std::string& fieldName) const
{
    for (const auto& groupConfig : _impl->groupConfigs) {
        if (groupConfig->GetFieldMode() == indexlib::config::SourceGroupConfig::SourceFieldMode::ALL_FIELD) {
            return groupConfig->GetGroupId();
        }
        if (groupConfig->GetFieldMode() == indexlib::config::SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD) {
            const auto& specifiedFields = groupConfig->GetSpecifiedFields();
            if (std::find(specifiedFields.begin(), specifiedFields.end(), fieldName) != specifiedFields.end()) {
                return groupConfig->GetGroupId();
            }
        }
    }
    return index::INVALID_SOURCEGROUPID;
}

} // namespace indexlibv2::config
