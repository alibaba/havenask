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
#include "indexlib/config/impl/summary_schema_impl.h"

#include <algorithm>

#include "autil/legacy/jsonizable.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, SummarySchemaImpl);

const size_t SummarySchemaImpl::INVALID_SUMMARY_POSITION = (size_t)-1;

SummarySchemaImpl::SummarySchemaImpl() : mNeedStoreSummary(true), mIsAllFieldsDisabled(false)
{
    mMaxFieldId = INVALID_FIELDID;
    summarygroupid_t defaultGroupId = CreateSummaryGroup(DEFAULT_SUMMARYGROUPNAME);
    assert(defaultGroupId == DEFAULT_SUMMARYGROUPID);
    (void)defaultGroupId;
}

std::shared_ptr<SummaryConfig> SummarySchemaImpl::GetSummaryConfig(fieldid_t fieldId) const
{
    if (!IsInSummary(fieldId)) {
        return std::shared_ptr<SummaryConfig>();
    }
    size_t pos = mFieldId2SummaryVec[fieldId];
    return mSummaryConfigs[pos];
}

void SummarySchemaImpl::AddSummaryConfig(const std::shared_ptr<SummaryConfig>& summaryConfig, summarygroupid_t groupId)
{
    if (IsAllFieldsDisabled()) {
        stringstream ss;
        ss << "summary all field is disabled, cannot add config: " + summaryConfig->GetSummaryName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    fieldid_t fieldId = summaryConfig->GetFieldConfig()->GetFieldId();
    if (IsInSummary(fieldId)) {
        stringstream ss;
        ss << "duplicated summary field: " + summaryConfig->GetSummaryName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    if (groupId + 1 != (summarygroupid_t)mSummaryGroups.size()) {
        INDEXLIB_FATAL_ERROR(Schema, "invalid summary groupId[%d], expect[%d]", groupId,
                             (summarygroupid_t)mSummaryGroups.size() - 1);
    }
    mSummaryGroups[groupId]->AddSummaryConfig(summaryConfig);

    AddSummaryPosition(fieldId, mSummaryConfigs.size(), groupId);
    mSummaryConfigs.push_back(summaryConfig);
}

void SummarySchemaImpl::AddSummaryPosition(fieldid_t fieldId, size_t pos, summarygroupid_t groupId)
{
    if (fieldId >= (fieldid_t)mFieldId2SummaryVec.size()) {
        mFieldId2SummaryVec.resize(fieldId + 1, SummarySchemaImpl::INVALID_SUMMARY_POSITION);
        mFieldId2GroupIdVec.resize(fieldId + 1, (size_t)INVALID_SUMMARYGROUPID);
    }
    mFieldId2SummaryVec[fieldId] = pos;
    mFieldId2GroupIdVec[fieldId] = (size_t)groupId;
    if (mMaxFieldId < fieldId) {
        mMaxFieldId = fieldId;
    }
}

void SummarySchemaImpl::AssertEqual(const SummarySchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mFieldId2SummaryVec, other.mFieldId2SummaryVec, "FieldId2SummaryMap not equal");
    IE_CONFIG_ASSERT_EQUAL(mSummaryConfigs.size(), other.mSummaryConfigs.size(), "mSummaryConfigs's size not equal");
    for (size_t i = 0; i < mSummaryConfigs.size(); ++i) {
        auto status = mSummaryConfigs[i]->CheckEqual(*(other.mSummaryConfigs[i]));
        THROW_IF_STATUS_ERROR(status);
    }
    IE_CONFIG_ASSERT_EQUAL(mSummaryGroups.size(), other.mSummaryGroups.size(), "mSummaryGroups's size not equal");
    for (size_t i = 0; i < mSummaryGroups.size(); ++i) {
        mSummaryGroups[i]->AssertEqual(*(other.mSummaryGroups[i]));
    }
    IE_CONFIG_ASSERT_EQUAL(mIsAllFieldsDisabled, other.mIsAllFieldsDisabled, "disable fields not equal");
}

void SummarySchemaImpl::AssertCompatible(const SummarySchemaImpl& other) const
{
    IE_CONFIG_ASSERT(mFieldId2SummaryVec.size() <= other.mFieldId2SummaryVec.size(),
                     "FieldId2SummaryMap's size not compatible");
    IE_CONFIG_ASSERT(mSummaryConfigs.size() <= other.mSummaryConfigs.size(), "mSummaryConfigs' size not compatible");
    for (size_t i = 0; i < mSummaryConfigs.size(); ++i) {
        auto status = mSummaryConfigs[i]->CheckEqual(*(other.mSummaryConfigs[i]));
        THROW_IF_STATUS_ERROR(status);
    }
    IE_CONFIG_ASSERT(mSummaryGroups.size() <= other.mSummaryGroups.size(), "mSummaryGroups's size not compatible");
    for (size_t i = 0; i < mSummaryGroups.size(); ++i) {
        mSummaryGroups[i]->AssertCompatible(*(other.mSummaryGroups[i]));
    }
    IE_CONFIG_ASSERT(mIsAllFieldsDisabled >= other.mIsAllFieldsDisabled, "disable fields not compatible");
}

void SummarySchemaImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        JsonMap summaryMap;
        // group 0
        assert(DEFAULT_SUMMARYGROUPID == 0);
        const SummaryGroupConfigPtr& defaultGroup = mSummaryGroups[0];
        SummaryGroupConfig::Iterator it = defaultGroup->Begin();
        JsonArray fieldNames;
        for (; it != defaultGroup->End(); ++it) {
            fieldNames.push_back((*it)->GetSummaryName());
        }
        summaryMap[SUMMARY_FIELDS] = fieldNames;
        if (defaultGroup->IsCompress()) {
            summaryMap[SUMMARY_COMPRESS] = true;
            if (!defaultGroup->GetCompressType().empty()) {
                summaryMap[index::COMPRESS_TYPE] = defaultGroup->GetCompressType();
            }
        }
        if (defaultGroup->HasEnableAdaptiveOffset()) {
            summaryMap[SUMMARY_ADAPTIVE_OFFSET] = true;
        }
        summaryMap[SUMMARY_GROUP_PARAMTETER] = ToJson(defaultGroup->GetSummaryGroupDataParam());

        // groups from 1
        if (GetSummaryGroupConfigCount() > 1) {
            JsonArray groups;
            for (size_t i = 1; i < mSummaryGroups.size(); ++i) {
                Any group = ToJson(mSummaryGroups[i]);
                groups.push_back(group);
            }
            summaryMap[SUMMARY_GROUPS] = groups;
        }
        json.Jsonize(SUMMARYS, summaryMap);
    }
}

std::shared_ptr<SummaryConfig> SummarySchemaImpl::GetSummaryConfig(const string& fieldName) const
{
    if (mIsAllFieldsDisabled) {
        return std::shared_ptr<SummaryConfig>();
    }
    for (size_t i = 0; i < mSummaryConfigs.size(); i++) {
        if (mSummaryConfigs[i]->GetSummaryName() == fieldName) {
            return mSummaryConfigs[i];
        }
    }
    return std::shared_ptr<SummaryConfig>();
}

void SummarySchemaImpl::SetNeedStoreSummary(bool needStoreSummary)
{
    mNeedStoreSummary = needStoreSummary;
    for (size_t i = 0; i < mSummaryGroups.size(); ++i) {
        mSummaryGroups[i]->SetNeedStoreSummary(needStoreSummary);
    }
}

void SummarySchemaImpl::SetNeedStoreSummary(fieldid_t fieldId)
{
    mNeedStoreSummary = true;
    if (fieldId == INVALID_FIELDID) {
        INDEXLIB_FATAL_ERROR(Schema, "invalid summary fieldId[%d]", fieldId);
    }
    summarygroupid_t gropuId = FieldIdToSummaryGroupId(fieldId);
    mSummaryGroups[gropuId]->SetNeedStoreSummary(true);
}

summarygroupid_t SummarySchemaImpl::CreateSummaryGroup(const string& groupName)
{
    assert(!mIsAllFieldsDisabled);
    if (mSummaryGroupIdMap.count(groupName) > 0) {
        INDEXLIB_FATAL_ERROR(Schema, "summary groupName[%s] already exists", groupName.c_str());
    }
    summarygroupid_t groupId = (summarygroupid_t)mSummaryGroups.size();
    mSummaryGroups.push_back(SummaryGroupConfigPtr(new SummaryGroupConfig(groupName, groupId, mSummaryConfigs.size())));
    mSummaryGroupIdMap.insert(make_pair(groupName, groupId));
    return groupId;
}

summarygroupid_t SummarySchemaImpl::FieldIdToSummaryGroupId(fieldid_t fieldId) const
{
    assert(!mIsAllFieldsDisabled);
    assert(mFieldId2GroupIdVec.size() == mFieldId2SummaryVec.size());
    if (fieldId >= (fieldid_t)mFieldId2GroupIdVec.size() || fieldId < 0) {
        return INVALID_SUMMARYGROUPID;
    }
    return (summaryfieldid_t)mFieldId2GroupIdVec[fieldId];
}

summarygroupid_t SummarySchemaImpl::GetSummaryGroupId(const std::string& groupName) const
{
    SummaryGroupNameToIdMap::const_iterator it = mSummaryGroupIdMap.find(groupName);
    if (it == mSummaryGroupIdMap.end()) {
        return INVALID_SUMMARYGROUPID;
    }
    return it->second;
}

const SummaryGroupConfigPtr& SummarySchemaImpl::GetSummaryGroupConfig(const string& groupName) const
{
    summarygroupid_t groupId = GetSummaryGroupId(groupName);
    assert(groupId != INVALID_SUMMARYGROUPID);
    return GetSummaryGroupConfig(groupId);
}

const SummaryGroupConfigPtr& SummarySchemaImpl::GetSummaryGroupConfig(summarygroupid_t groupId) const
{
    assert(groupId >= 0 && groupId < (summarygroupid_t)mSummaryGroups.size());
    return mSummaryGroups[groupId];
}

summarygroupid_t SummarySchemaImpl::GetSummaryGroupConfigCount() const
{
    return (summarygroupid_t)mSummaryGroups.size();
}

void SummarySchemaImpl::DisableAllFields() { mIsAllFieldsDisabled = true; }

bool SummarySchemaImpl::IsAllFieldsDisabled() const { return mIsAllFieldsDisabled; }
}} // namespace indexlib::config
