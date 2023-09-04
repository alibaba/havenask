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
#include "indexlib/index/common/Types.h"
#include "indexlib/index/summary/Types.h"

namespace indexlib::config {
class SummaryConfig;
} // namespace indexlib::config

namespace indexlibv2::config {
class FieldConfig;
class SummaryGroupConfig;

class SummaryIndexConfig : public IIndexConfig
{
private:
    typedef std::vector<std::shared_ptr<indexlib::config::SummaryConfig>> SummaryConfigVector;

public:
    typedef SummaryConfigVector::const_iterator Iterator;

public:
    SummaryIndexConfig();
    explicit SummaryIndexConfig(const std::vector<std::shared_ptr<SummaryGroupConfig>>& groupConfigs);
    ~SummaryIndexConfig();

public:
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    const std::string& GetIndexCommonPath() const override;
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override;
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    void Check() const override;
    Status CheckCompatible(const IIndexConfig* other) const override;
    bool IsDisabled() const override;

public:
    std::shared_ptr<indexlib::config::SummaryConfig> GetSummaryConfig(fieldid_t fieldId) const;
    std::shared_ptr<indexlib::config::SummaryConfig> GetSummaryConfig(const std::string& fieldName) const;
    Status AddSummaryConfig(const std::shared_ptr<indexlib::config::SummaryConfig>& summaryConfig,
                            indexlib::index::summarygroupid_t groupId);
    size_t GetSummaryCount() const;
    bool IsInSummary(fieldid_t fieldId) const;

    index::summaryfieldid_t GetSummaryFieldId(fieldid_t fieldId) const;

    bool NeedStoreSummary();
    void SetNeedStoreSummary(bool needStoreSummary);
    void SetNeedStoreSummary(fieldid_t fieldId);

    void DisableAllFields();
    bool IsAllFieldsDisabled() const;

    Iterator begin();
    Iterator end();

public:
    // Group
    // Refer to INVALID_SUMMARYGROUPID, DEFAULT_SUMMARYGROUPID, DEFAULT_SUMMARYGROUPNAME
    std::pair<Status, indexlib::index::summarygroupid_t> CreateSummaryGroup(const std::string& groupName);
    indexlib::index::summarygroupid_t FieldIdToSummaryGroupId(fieldid_t fieldId) const;
    indexlib::index::summarygroupid_t GetSummaryGroupId(const std::string& groupName) const;
    const std::shared_ptr<SummaryGroupConfig>& GetSummaryGroupConfig(const std::string& groupName) const;
    const std::shared_ptr<SummaryGroupConfig>& GetSummaryGroupConfig(indexlib::index::summarygroupid_t groupId) const;
    indexlib::index::summarygroupid_t GetSummaryGroupConfigCount() const;

private:
    void AddSummaryPosition(fieldid_t fieldId, size_t pos, indexlib::index::summarygroupid_t groupId);
    bool LoadSummaryGroup(const autil::legacy::Any& any, index::summarygroupid_t groupId,
                          const config::IndexConfigDeserializeResource& resource);

private:
    typedef std::vector<size_t> FieldId2SummaryVec;
    typedef std::vector<std::shared_ptr<SummaryGroupConfig>> SummaryGroupConfigVec;
    typedef std::unordered_map<std::string, indexlib::index::summarygroupid_t> SummaryGroupNameToIdMap;

private:
    inline static const size_t INVALID_SUMMARY_POSITION = (size_t)-1;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
