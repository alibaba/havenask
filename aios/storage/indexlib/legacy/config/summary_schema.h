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

namespace indexlibv2::config {
class SummaryIndexConfig;
}

namespace indexlib { namespace config {
class SummarySchemaImpl;
typedef std::shared_ptr<SummarySchemaImpl> SummarySchemaImplPtr;

class SummarySchema : public autil::legacy::Jsonizable
{
private:
    typedef std::vector<std::shared_ptr<SummaryConfig>> SummaryConfigVector;

public:
    typedef SummaryConfigVector::const_iterator Iterator;

public:
    SummarySchema();
    ~SummarySchema() {};

public:
    std::shared_ptr<SummaryConfig> GetSummaryConfig(fieldid_t fieldId) const;
    std::shared_ptr<SummaryConfig> GetSummaryConfig(const std::string& fieldName) const;
    void AddSummaryConfig(const std::shared_ptr<SummaryConfig>& summaryConfig,
                          index::summarygroupid_t groupId = index::DEFAULT_SUMMARYGROUPID);
    size_t GetSummaryCount() const;

    Iterator Begin() const;
    Iterator End() const;

    bool IsInSummary(fieldid_t fieldId) const;

    index::summaryfieldid_t GetSummaryFieldId(fieldid_t fieldId) const;

    fieldid_t GetMaxFieldId() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const SummarySchema& other) const;
    void AssertCompatible(const SummarySchema& other) const;

    bool NeedStoreSummary();
    void SetNeedStoreSummary(bool needStoreSummary);
    void SetNeedStoreSummary(fieldid_t fieldId);

    void DisableAllFields();
    bool IsAllFieldsDisabled() const;

public:
    // Group
    // Refer to INVALID_SUMMARYGROUPID, index::DEFAULT_SUMMARYGROUPID, index::DEFAULT_SUMMARYGROUPNAME
    index::summarygroupid_t CreateSummaryGroup(const std::string& groupName);
    index::summarygroupid_t FieldIdToSummaryGroupId(fieldid_t fieldId) const;
    index::summarygroupid_t GetSummaryGroupId(const std::string& groupName) const;
    const SummaryGroupConfigPtr& GetSummaryGroupConfig(const std::string& groupName) const;
    const SummaryGroupConfigPtr& GetSummaryGroupConfig(index::summarygroupid_t groupId) const;
    index::summarygroupid_t GetSummaryGroupConfigCount() const;

public:
    std::unique_ptr<indexlibv2::config::SummaryIndexConfig> MakeSummaryIndexConfigV2();

private:
    SummarySchemaImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummarySchema> SummarySchemaPtr;
}} // namespace indexlib::config
