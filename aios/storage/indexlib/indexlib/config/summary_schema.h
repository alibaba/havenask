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
#ifndef __INDEXLIB_SUMMARY_SCHEMA_H
#define __INDEXLIB_SUMMARY_SCHEMA_H

#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/summary_group_config.h"
#include "indexlib/index/summary/config/SummaryConfig.h"
#include "indexlib/indexlib.h"

namespace indexlibv2::config {
class SummaryIndexConfig;
}

namespace indexlib { namespace config {
class SummarySchemaImpl;
DEFINE_SHARED_PTR(SummarySchemaImpl);

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
                          summarygroupid_t groupId = DEFAULT_SUMMARYGROUPID);
    size_t GetSummaryCount() const;

    Iterator Begin() const;
    Iterator End() const;

    bool IsInSummary(fieldid_t fieldId) const;

    summaryfieldid_t GetSummaryFieldId(fieldid_t fieldId) const;

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
    // Refer to INVALID_SUMMARYGROUPID, DEFAULT_SUMMARYGROUPID, DEFAULT_SUMMARYGROUPNAME
    summarygroupid_t CreateSummaryGroup(const std::string& groupName);
    summarygroupid_t FieldIdToSummaryGroupId(fieldid_t fieldId) const;
    summarygroupid_t GetSummaryGroupId(const std::string& groupName) const;
    const SummaryGroupConfigPtr& GetSummaryGroupConfig(const std::string& groupName) const;
    const SummaryGroupConfigPtr& GetSummaryGroupConfig(summarygroupid_t groupId) const;
    summarygroupid_t GetSummaryGroupConfigCount() const;

public:
    std::unique_ptr<indexlibv2::config::SummaryIndexConfig> MakeSummaryIndexConfigV2();

private:
    SummarySchemaImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummarySchema);
}} // namespace indexlib::config

#endif //__INDEXLIB_SUMMARY_SCHEMA_H
