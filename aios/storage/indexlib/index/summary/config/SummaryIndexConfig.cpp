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
#include "indexlib/index/summary/config/SummaryIndexConfig.h"

#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/SingleFileCompressConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/config/SummaryConfig.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using autil::legacy::Any;
using autil::legacy::AnyCast;
using autil::legacy::json::JsonArray;
using autil::legacy::json::JsonMap;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, SummaryIndexConfig);

struct SummaryIndexConfig::Impl {
    SummaryConfigVector summaryConfigs;
    FieldId2SummaryVec fieldId2SummaryVec;
    SummaryGroupConfigVec summaryGroups;
    SummaryGroupNameToIdMap summaryGroupIdMap;
    FieldId2SummaryVec fieldId2GroupIdVec;
    bool needStoreSummary = true; // true: if all fields in attributes
    bool isAllFieldsDisabled = false;
};

SummaryIndexConfig::SummaryIndexConfig() : _impl(std::make_unique<Impl>())
{
    [[maybe_unused]] auto [status, defaultGroupId] = CreateSummaryGroup(index::DEFAULT_SUMMARYGROUPNAME);
    assert(defaultGroupId == index::DEFAULT_SUMMARYGROUPID);
}

SummaryIndexConfig::SummaryIndexConfig(const std::vector<std::shared_ptr<SummaryGroupConfig>>& groupConfigs)
    : _impl(std::make_unique<Impl>())
{
    assert(!groupConfigs.empty());
    for (auto groupConfig : groupConfigs) {
        auto groupId = groupConfig->GetGroupId();
        if (groupId != _impl->summaryGroups.size()) {
            AUTIL_LOG(ERROR, "invalid summary group id [%d], expect[%lu]", groupId, _impl->summaryGroups.size());
            assert(false);
            continue;
        }
        const auto& groupName = groupConfig->GetGroupName();
        if (_impl->summaryGroupIdMap.find(groupName) != _impl->summaryGroupIdMap.end()) {
            AUTIL_LOG(ERROR, "duplicate summary group name found [%s]", groupName.c_str());
            assert(false);
            continue;
        }
        _impl->summaryGroups.emplace_back(groupConfig);
        _impl->summaryGroupIdMap[groupName] = groupId;
        assert(!IsAllFieldsDisabled());
        for (auto it = groupConfig->Begin(); it != groupConfig->End(); ++it) {
            auto summaryConfig = *it;
            auto fieldId = summaryConfig->GetFieldId();
            if (IsInSummary(fieldId)) {
                AUTIL_LOG(ERROR, "duplicated summary field: %s", summaryConfig->GetSummaryName().c_str());
                assert(false);
                continue;
            }
            AddSummaryPosition(fieldId, _impl->summaryConfigs.size(), groupId);
            _impl->summaryConfigs.push_back(summaryConfig);
        }
    }
}

SummaryIndexConfig::~SummaryIndexConfig() {}

std::vector<std::shared_ptr<FieldConfig>> SummaryIndexConfig::GetFieldConfigs() const
{
    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    for (auto summaryConf : _impl->summaryConfigs) {
        fieldConfigs.emplace_back(summaryConf->GetFieldConfig());
    }
    return fieldConfigs;
}

const std::string& SummaryIndexConfig::GetIndexType() const { return indexlibv2::index::SUMMARY_INDEX_TYPE_STR; }

const std::string& SummaryIndexConfig::GetIndexName() const { return indexlibv2::index::SUMMARY_INDEX_NAME; }

std::vector<std::string> SummaryIndexConfig::GetIndexPath() const { return {index::SUMMARY_INDEX_PATH}; }

std::shared_ptr<indexlib::config::SummaryConfig> SummaryIndexConfig::GetSummaryConfig(fieldid_t fieldId) const
{
    if (!IsInSummary(fieldId)) {
        return nullptr;
    }
    size_t pos = _impl->fieldId2SummaryVec[fieldId];
    return _impl->summaryConfigs[pos];
}
std::shared_ptr<indexlib::config::SummaryConfig>
SummaryIndexConfig::GetSummaryConfig(const std::string& fieldName) const
{
    if (_impl->isAllFieldsDisabled) {
        return nullptr;
    }
    for (auto summaryConf : _impl->summaryConfigs) {
        if (summaryConf->GetSummaryName() == fieldName) {
            return summaryConf;
        }
    }
    return nullptr;
}
Status SummaryIndexConfig::AddSummaryConfig(const std::shared_ptr<indexlib::config::SummaryConfig>& summaryConfig,
                                            indexlib::index::summarygroupid_t groupId)
{
    if (IsAllFieldsDisabled()) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "summary all field is disabled, cannot add config: %s",
                               summaryConfig->GetSummaryName().c_str());
    }
    fieldid_t fieldId = summaryConfig->GetFieldId();
    if (IsInSummary(fieldId)) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "duplicated summary field: %s",
                               summaryConfig->GetSummaryName().c_str());
    }

    if (groupId + 1 != (indexlib::index::summarygroupid_t)_impl->summaryGroups.size()) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "invalid summary groupId[%d], expect[%d]", groupId,
                               (indexlib::index::summarygroupid_t)_impl->summaryGroups.size() - 1);
    }
    _impl->summaryGroups[groupId]->AddSummaryConfig(summaryConfig);

    AddSummaryPosition(fieldId, _impl->summaryConfigs.size(), groupId);
    _impl->summaryConfigs.push_back(summaryConfig);
    return Status::OK();
}
size_t SummaryIndexConfig::GetSummaryCount() const { return _impl->summaryConfigs.size(); }

bool SummaryIndexConfig::IsInSummary(fieldid_t fieldId) const
{
    if (_impl->isAllFieldsDisabled) {
        return false;
    }
    if (fieldId >= (fieldid_t)_impl->fieldId2SummaryVec.size() || fieldId == INVALID_FIELDID) {
        return false;
    }
    return _impl->fieldId2SummaryVec[fieldId] != INVALID_SUMMARY_POSITION;
}

index::summaryfieldid_t SummaryIndexConfig::GetSummaryFieldId(fieldid_t fieldId) const
{
    assert(!_impl->isAllFieldsDisabled);
    if (!IsInSummary(fieldId)) {
        return index::INVALID_SUMMARYFIELDID;
    }
    return (index::summaryfieldid_t)_impl->fieldId2SummaryVec[fieldId];
}

bool SummaryIndexConfig::NeedStoreSummary() { return !IsAllFieldsDisabled() && _impl->needStoreSummary; }
void SummaryIndexConfig::SetNeedStoreSummary(bool needStoreSummary)
{
    _impl->needStoreSummary = needStoreSummary;
    for (size_t i = 0; i < _impl->summaryGroups.size(); ++i) {
        _impl->summaryGroups[i]->SetNeedStoreSummary(needStoreSummary);
    }
}
void SummaryIndexConfig::SetNeedStoreSummary(fieldid_t fieldId)
{
    _impl->needStoreSummary = true;
    if (fieldId == INVALID_FIELDID) {
        AUTIL_LOG(ERROR, "invalid summary fieldId[%d]", fieldId);
        assert(false);
    }
    indexlib::index::summarygroupid_t groupId = FieldIdToSummaryGroupId(fieldId);
    assert(groupId >= 0 && groupId < _impl->summaryGroups.size());
    _impl->summaryGroups[groupId]->SetNeedStoreSummary(true);
}

std::pair<Status, indexlib::index::summarygroupid_t>
SummaryIndexConfig::CreateSummaryGroup(const std::string& groupName)
{
    assert(!_impl->isAllFieldsDisabled);
    if (_impl->summaryGroupIdMap.count(groupName) > 0) {
        RETURN2_IF_STATUS_ERROR(Status::ConfigError(), index::INVALID_SUMMARYGROUPID,
                                "summary groupName[%s] already exists", groupName.c_str());
    }
    indexlib::index::summarygroupid_t groupId = (indexlib::index::summarygroupid_t)_impl->summaryGroups.size();
    _impl->summaryGroups.push_back(
        std::make_shared<SummaryGroupConfig>(groupName, groupId, _impl->summaryConfigs.size()));
    _impl->summaryGroupIdMap.insert(std::make_pair(groupName, groupId));
    return {Status::OK(), groupId};
}

indexlib::index::summarygroupid_t SummaryIndexConfig::FieldIdToSummaryGroupId(fieldid_t fieldId) const
{
    assert(!_impl->isAllFieldsDisabled);
    assert(_impl->fieldId2GroupIdVec.size() == _impl->fieldId2SummaryVec.size());
    if (fieldId >= (fieldid_t)_impl->fieldId2GroupIdVec.size() || fieldId < 0) {
        return index::INVALID_SUMMARYGROUPID;
    }
    return (index::summaryfieldid_t)_impl->fieldId2GroupIdVec[fieldId];
}
indexlib::index::summarygroupid_t SummaryIndexConfig::GetSummaryGroupId(const std::string& groupName) const
{
    auto it = _impl->summaryGroupIdMap.find(groupName);
    if (it == _impl->summaryGroupIdMap.end()) {
        return index::INVALID_SUMMARYGROUPID;
    }
    return it->second;
}
const std::shared_ptr<SummaryGroupConfig>& SummaryIndexConfig::GetSummaryGroupConfig(const std::string& groupName) const
{
    indexlib::index::summarygroupid_t groupId = GetSummaryGroupId(groupName);
    assert(groupId != index::INVALID_SUMMARYGROUPID);
    return GetSummaryGroupConfig(groupId);
}
const std::shared_ptr<SummaryGroupConfig>&
SummaryIndexConfig::GetSummaryGroupConfig(indexlib::index::summarygroupid_t groupId) const
{
    assert(groupId >= 0 && groupId < (indexlib::index::summarygroupid_t)_impl->summaryGroups.size());
    return _impl->summaryGroups[groupId];
}
indexlib::index::summarygroupid_t SummaryIndexConfig::GetSummaryGroupConfigCount() const
{
    return (indexlib::index::summarygroupid_t)_impl->summaryGroups.size();
}

void SummaryIndexConfig::DisableAllFields() { _impl->isAllFieldsDisabled = true; }

bool SummaryIndexConfig::IsAllFieldsDisabled() const { return _impl->isAllFieldsDisabled; }

SummaryIndexConfig::Iterator SummaryIndexConfig::begin() { return _impl->summaryConfigs.begin(); }
SummaryIndexConfig::Iterator SummaryIndexConfig::end() { return _impl->summaryConfigs.end(); }

void SummaryIndexConfig::Check() const
{
    // un-implemented
}

void SummaryIndexConfig::AddSummaryPosition(fieldid_t fieldId, size_t pos, indexlib::index::summarygroupid_t groupId)
{
    if (fieldId >= (fieldid_t)_impl->fieldId2SummaryVec.size()) {
        _impl->fieldId2SummaryVec.resize(fieldId + 1, INVALID_SUMMARY_POSITION);
        _impl->fieldId2GroupIdVec.resize(fieldId + 1, (size_t)index::INVALID_SUMMARYGROUPID);
    }
    _impl->fieldId2SummaryVec[fieldId] = pos;
    _impl->fieldId2GroupIdVec[fieldId] = (size_t)groupId;
}

void SummaryIndexConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                     const config::IndexConfigDeserializeResource& resource)
{
    if (!LoadSummaryGroup(any, index::DEFAULT_SUMMARYGROUPID, resource)) {
        return;
    }
    // non-default summary group
    JsonMap summary = AnyCast<JsonMap>(any);
    JsonMap::const_iterator it = summary.find(indexlib::index::SUMMARY_GROUPS);
    if (it != summary.end()) {
        JsonArray summaryGroups = AnyCast<JsonArray>(it->second);
        for (JsonArray::iterator iter = summaryGroups.begin(); iter != summaryGroups.end(); ++iter) {
            JsonMap group = AnyCast<JsonMap>(*iter);
            JsonMap::const_iterator groupIt = group.find(indexlib::index::SUMMARY_GROUP_NAME);
            if (groupIt == group.end()) {
                INDEXLIB_FATAL_ERROR(Schema, "summary group has no name");
            }
            std::string groupName = AnyCast<std::string>(groupIt->second);
            auto [status, groupId] = CreateSummaryGroup(groupName);
            THROW_IF_STATUS_ERROR(status);
            if (!LoadSummaryGroup(group, groupId, resource)) {
                INDEXLIB_FATAL_ERROR(Schema, "load summary groupName[%s] failed", groupName.c_str());
            }
        }
    }
}

void SummaryIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    // group 0
    const auto& defaultGroup = GetSummaryGroupConfig(index::DEFAULT_SUMMARYGROUPID);
    SummaryGroupConfig::Iterator it = defaultGroup->Begin();
    JsonArray fieldNames;
    for (; it != defaultGroup->End(); ++it) {
        fieldNames.push_back((*it)->GetSummaryName());
    }
    json.Jsonize(indexlib::index::SUMMARY_FIELDS, fieldNames);
    if (defaultGroup->HasEnableAdaptiveOffset()) {
        json.Jsonize(indexlib::index::SUMMARY_ADAPTIVE_OFFSET, true);
    }
    json.Jsonize(indexlib::index::SUMMARY_GROUP_PARAMTETER, defaultGroup->GetSummaryGroupDataParam());

    // groups from 1
    auto groupCount = GetSummaryGroupConfigCount();
    if (groupCount > 1) {
        JsonArray groups;
        for (size_t i = 1; i < groupCount; ++i) {
            Any group = autil::legacy::ToJson(GetSummaryGroupConfig(i));
            groups.push_back(group);
        }
        json.Jsonize(indexlib::index::SUMMARY_GROUPS, groups);
    }
}

bool SummaryIndexConfig::LoadSummaryGroup(const Any& any, index::summarygroupid_t groupId,
                                          const config::IndexConfigDeserializeResource& resource)
{
    auto summary = AnyCast<JsonMap>(any);
    auto json = autil::legacy::Jsonizable::JsonWrapper(summary);
    std::vector<std::string> summaryFields;
    json.Jsonize(indexlib::index::SUMMARY_FIELDS, summaryFields, summaryFields);
    if (summaryFields.empty()) {
        AUTIL_LOG(INFO, "summary group[%d] fields empty", groupId);
        return false;
    }
    for (const auto& fieldName : summaryFields) {
        auto fieldConfig = resource.GetFieldConfig(fieldName);
        if (!fieldConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "summary field[%s] not found in fields", fieldName.c_str());
        }
        auto summaryConfig = std::make_shared<indexlib::config::SummaryConfig>();
        summaryConfig->SetFieldConfig(fieldConfig);
        auto status = AddSummaryConfig(summaryConfig, groupId);
        THROW_IF_STATUS_ERROR(status);
    }
    auto groupConfig = GetSummaryGroupConfig(groupId);
    assert(groupConfig);
    auto iter = summary.find(indexlib::index::SUMMARY_GROUP_PARAMTETER);
    if (iter != summary.end()) {
        autil::legacy::Jsonizable::JsonWrapper wrapper(iter->second);
        indexlib::config::GroupDataParameter param;
        param.Jsonize(wrapper);
        const auto& fileCompressor = param.GetFileCompressor();

        if (!fileCompressor.empty() && !param.GetFileCompressConfigV2()) {
            const auto& fileCompressName = param.GetFileCompressName();
            if (!fileCompressName.empty()) {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "v2 summary config use 'file_compressor' instead of 'file_compress', group[%s]",
                                     groupConfig->GetGroupName().c_str());
            }

            auto fileCompressConfigV2 = resource.GetFileCompressConfig(fileCompressor);
            if (!fileCompressConfigV2) {
                INDEXLIB_FATAL_ERROR(Schema, "get file compress[%s] failed", fileCompressor.c_str());
            }
            param.SetFileCompressConfigV2(fileCompressConfigV2);
        }
        groupConfig->SetSummaryGroupDataParam(param);
    }
    iter = summary.find(indexlib::index::SUMMARY_ADAPTIVE_OFFSET);
    if (iter != summary.end()) {
        bool adaptiveOffset = AnyCast<bool>(iter->second);
        groupConfig->SetEnableAdaptiveOffset(adaptiveOffset);
    }
    return true;
}

Status SummaryIndexConfig::CheckCompatible(const IIndexConfig* other) const
{
    const auto* typedOther = dynamic_cast<const SummaryIndexConfig*>(other);
    if (!typedOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "cast to SummaryIndexConfig failed");
    }
    auto toJsonString = [](const config::IIndexConfig* config) {
        autil::legacy::Jsonizable::JsonWrapper json;
        config->Serialize(json);
        return ToJsonString(json.GetMap());
    };
    const auto& jsonStr = toJsonString(this);
    const auto& jsonStrOther = toJsonString(typedOther);
    if (jsonStr != jsonStrOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "original config [%s] is not compatible with [%s]",
                               jsonStr.c_str(), jsonStrOther.c_str());
    }
    return Status::OK();
}

} // namespace indexlibv2::config
