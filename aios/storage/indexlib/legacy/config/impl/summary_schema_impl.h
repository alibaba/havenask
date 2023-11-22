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
#include <unordered_map>
#include <vector>

#include "autil/Log.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/summary_group_config.h"
#include "indexlib/index/summary/config/SummaryConfig.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class SummarySchemaImpl : public autil::legacy::Jsonizable
{
private:
    typedef std::vector<std::shared_ptr<SummaryConfig>> SummaryConfigVector;

public:
    typedef SummaryConfigVector::const_iterator Iterator;

public:
    SummarySchemaImpl();
    ~SummarySchemaImpl() {};

public:
    std::shared_ptr<SummaryConfig> GetSummaryConfig(fieldid_t fieldId) const;
    std::shared_ptr<SummaryConfig> GetSummaryConfig(const std::string& fieldName) const;
    void AddSummaryConfig(const std::shared_ptr<SummaryConfig>& summaryConfig,
                          index::summarygroupid_t groupId = index::DEFAULT_SUMMARYGROUPID);
    size_t GetSummaryCount() const { return mSummaryConfigs.size(); }

    Iterator Begin() const { return mSummaryConfigs.begin(); }
    Iterator End() const { return mSummaryConfigs.end(); }

    bool IsInSummary(fieldid_t fieldId) const
    {
        if (mIsAllFieldsDisabled) {
            return false;
        }
        if (fieldId >= (fieldid_t)mFieldId2SummaryVec.size() || fieldId == INVALID_FIELDID) {
            return false;
        }
        return mFieldId2SummaryVec[fieldId] != SummarySchemaImpl::INVALID_SUMMARY_POSITION;
    }

    index::summaryfieldid_t GetSummaryFieldId(fieldid_t fieldId) const
    {
        assert(!mIsAllFieldsDisabled);
        if (!IsInSummary(fieldId)) {
            return index::INVALID_SUMMARYFIELDID;
        }
        return (index::summaryfieldid_t)mFieldId2SummaryVec[fieldId];
    }

    fieldid_t GetMaxFieldId() const
    {
        assert(!mIsAllFieldsDisabled);
        return mMaxFieldId;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const SummarySchemaImpl& other) const;
    void AssertCompatible(const SummarySchemaImpl& other) const;

    bool NeedStoreSummary() { return !IsAllFieldsDisabled() && mNeedStoreSummary; }
    void SetNeedStoreSummary(bool needStoreSummary);
    void SetNeedStoreSummary(fieldid_t fieldId);

    void DisableAllFields();
    bool IsAllFieldsDisabled() const;

public:
    // Group
    // Refer to index::INVALID_SUMMARYGROUPID, index::DEFAULT_SUMMARYGROUPID, index::DEFAULT_SUMMARYGROUPNAME
    index::summarygroupid_t CreateSummaryGroup(const std::string& groupName);
    index::summarygroupid_t FieldIdToSummaryGroupId(fieldid_t fieldId) const;
    index::summarygroupid_t GetSummaryGroupId(const std::string& groupName) const;
    const SummaryGroupConfigPtr& GetSummaryGroupConfig(const std::string& groupName) const;
    const SummaryGroupConfigPtr& GetSummaryGroupConfig(index::summarygroupid_t groupId) const;
    index::summarygroupid_t GetSummaryGroupConfigCount() const;

private:
    void AddSummaryPosition(fieldid_t fieldId, size_t pos, index::summarygroupid_t groupId);

private:
    typedef std::vector<size_t> FieldId2SummaryVec;
    typedef std::vector<SummaryGroupConfigPtr> SummaryGroupConfigVec;
    typedef std::unordered_map<std::string, index::summarygroupid_t> SummaryGroupNameToIdMap;

private:
    SummaryConfigVector mSummaryConfigs;
    FieldId2SummaryVec mFieldId2SummaryVec;
    SummaryGroupConfigVec mSummaryGroups;
    SummaryGroupNameToIdMap mSummaryGroupIdMap;
    FieldId2SummaryVec mFieldId2GroupIdVec;
    fieldid_t mMaxFieldId;
    bool mNeedStoreSummary; // true: if all fields in attributes
    bool mIsAllFieldsDisabled;

private:
    static const size_t INVALID_SUMMARY_POSITION;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummarySchemaImpl> SummarySchemaImplPtr;
}} // namespace indexlib::config
