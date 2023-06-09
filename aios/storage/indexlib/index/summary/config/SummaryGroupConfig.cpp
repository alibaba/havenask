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
#include "indexlib/index/summary/config/SummaryGroupConfig.h"

#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/config/SummaryConfig.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, SummaryGroupConfig);

struct SummaryGroupConfig::Impl {
    SummaryConfigVec summaryConfigs;
    std::string groupName;
    indexlib::config::GroupDataParameter parameter;
    index::summarygroupid_t groupId = index::INVALID_SUMMARYGROUPID;
    index::summaryfieldid_t summaryFieldIdBase = index::INVALID_SUMMARYFIELDID;
    bool needStoreSummary = false; // true: if all fields in attributes
    bool enableAdaptiveOffset = false;

    Impl() {}
    Impl(const std::string& name, index::summarygroupid_t id, index::summaryfieldid_t idBase)
        : groupName(name)
        , groupId(id)
        , summaryFieldIdBase(idBase)
    {
    }
};

SummaryGroupConfig::SummaryGroupConfig() : _impl(std::make_unique<Impl>()) {}

SummaryGroupConfig::SummaryGroupConfig(const std::string& groupName, index::summarygroupid_t groupId,
                                       index::summaryfieldid_t summaryFieldIdBase)
    : _impl(std::make_unique<Impl>(groupName, groupId, summaryFieldIdBase))
{
}
SummaryGroupConfig::~SummaryGroupConfig() {}

const std::string& SummaryGroupConfig::GetGroupName() { return _impl->groupName; }

void SummaryGroupConfig::SetGroupName(const std::string& groupName) { _impl->groupName = groupName; }

index::summarygroupid_t SummaryGroupConfig::GetGroupId() const { return _impl->groupId; }

bool SummaryGroupConfig::IsDefaultGroup() const { return _impl->groupId == index::DEFAULT_SUMMARYGROUPID; }

void SummaryGroupConfig::SetCompress(bool useCompress, const std::string& compressType)
{
    _impl->parameter.SetDocCompressor(useCompress ? compressType : "");
}

bool SummaryGroupConfig::IsCompress() const { return !_impl->parameter.GetDocCompressor().empty(); }

const std::string& SummaryGroupConfig::GetCompressType() const { return _impl->parameter.GetDocCompressor(); }

bool SummaryGroupConfig::NeedStoreSummary() { return _impl->needStoreSummary; }

void SummaryGroupConfig::SetNeedStoreSummary(bool needStoreSummary) { _impl->needStoreSummary = needStoreSummary; }

index::summaryfieldid_t SummaryGroupConfig::GetSummaryFieldIdBase() const { return _impl->summaryFieldIdBase; }

void SummaryGroupConfig::AddSummaryConfig(const std::shared_ptr<indexlib::config::SummaryConfig>& summaryConfig)
{
    _impl->summaryConfigs.push_back(summaryConfig);
}

Status SummaryGroupConfig::CheckEqual(const SummaryGroupConfig& other) const
{
    CHECK_CONFIG_EQUAL(_impl->summaryConfigs.size(), other._impl->summaryConfigs.size(),
                       "summaryConfigs's size not equal");
    for (size_t i = 0; i < _impl->summaryConfigs.size(); ++i) {
        auto status = _impl->summaryConfigs[i]->CheckEqual(*(other._impl->summaryConfigs[i]));
        RETURN_IF_STATUS_ERROR(status, "summary config [%lu] not equal", i);
    }
    CHECK_CONFIG_EQUAL(_impl->groupName, other._impl->groupName, "groupName not equal");
    CHECK_CONFIG_EQUAL(_impl->groupId, other._impl->groupId, "groupId not equal");
    CHECK_CONFIG_EQUAL(_impl->enableAdaptiveOffset, other._impl->enableAdaptiveOffset,
                       "enableAdaptiveOffset not equal");
    auto status = _impl->parameter.CheckEqual(other._impl->parameter);
    RETURN_IF_STATUS_ERROR(status, "group_data_parameter not equal");
    return Status::OK();
}

Status SummaryGroupConfig::CheckCompatible(const SummaryGroupConfig& other) const
{
    if (_impl->summaryConfigs.size() > other._impl->summaryConfigs.size()) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "summaryConfigs' size not compatible");
    }
    for (size_t i = 0; i < _impl->summaryConfigs.size(); ++i) {
        auto status = _impl->summaryConfigs[i]->CheckEqual(*(other._impl->summaryConfigs[i]));
        RETURN_IF_STATUS_ERROR(status, "summary config [%lu] not equal", i);
    }
    CHECK_CONFIG_EQUAL(_impl->groupName, other._impl->groupName, "groupName not equal");
    CHECK_CONFIG_EQUAL(_impl->groupId, other._impl->groupId, "groupId not equal");
    return Status::OK();
}

void SummaryGroupConfig::SetSummaryGroupDataParam(const indexlib::config::GroupDataParameter& param)
{
    _impl->parameter = param;
}

const indexlib::config::GroupDataParameter& SummaryGroupConfig::GetSummaryGroupDataParam() const
{
    return _impl->parameter;
}

bool SummaryGroupConfig::HasEnableAdaptiveOffset() const { return _impl->enableAdaptiveOffset; }

void SummaryGroupConfig::SetEnableAdaptiveOffset(bool enableFlag) { _impl->enableAdaptiveOffset = enableFlag; }

SummaryGroupConfig::Iterator SummaryGroupConfig::Begin() const { return _impl->summaryConfigs.begin(); }

SummaryGroupConfig::Iterator SummaryGroupConfig::End() const { return _impl->summaryConfigs.end(); }

size_t SummaryGroupConfig::GetSummaryFieldsCount() const { return _impl->summaryConfigs.size(); }

void SummaryGroupConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    std::vector<std::string> summaryFields;
    summaryFields.reserve(_impl->summaryConfigs.size());
    for (size_t i = 0; i < _impl->summaryConfigs.size(); ++i) {
        summaryFields.push_back(_impl->summaryConfigs[i]->GetSummaryName());
    }
    json.Jsonize(indexlib::index::SUMMARY_FIELDS, summaryFields);
    json.Jsonize(indexlib::index::SUMMARY_GROUP_NAME, _impl->groupName);
    json.Jsonize(indexlib::index::SUMMARY_GROUP_PARAMTETER, _impl->parameter);
    if (_impl->enableAdaptiveOffset) {
        json.Jsonize(indexlib::index::SUMMARY_ADAPTIVE_OFFSET, _impl->enableAdaptiveOffset);
    }
}

} // namespace indexlibv2::config
