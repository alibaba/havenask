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
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/summary/Types.h"

namespace indexlib::config {
class GroupDataParameter;
class SummaryConfig;
} // namespace indexlib::config

namespace indexlibv2::config {

class SummaryGroupConfig : public autil::legacy::Jsonizable
{
public:
    SummaryGroupConfig();
    SummaryGroupConfig(const std::string& groupName, index::summarygroupid_t groupId,
                       index::summaryfieldid_t summaryFieldIdBase);
    ~SummaryGroupConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const std::string& GetGroupName();
    index::summarygroupid_t GetGroupId() const;
    bool IsDefaultGroup() const;
    virtual bool IsCompress() const;
    virtual const std::string& GetCompressType() const;
    bool NeedStoreSummary();
    index::summaryfieldid_t GetSummaryFieldIdBase() const;
    Status CheckEqual(const SummaryGroupConfig& other) const;
    Status CheckCompatible(const SummaryGroupConfig& other) const;
    const indexlib::config::GroupDataParameter& GetSummaryGroupDataParam() const;
    bool HasEnableAdaptiveOffset() const;

public:
    virtual void SetGroupName(const std::string& groupName);
    virtual void SetCompress(bool useCompress, const std::string& compressType);
    virtual void SetNeedStoreSummary(bool needStoreSummary);
    virtual void AddSummaryConfig(const std::shared_ptr<indexlib::config::SummaryConfig>& summaryConfig);
    virtual void SetSummaryGroupDataParam(const indexlib::config::GroupDataParameter& param);
    virtual void SetEnableAdaptiveOffset(bool enableFlag);

public:
    typedef std::vector<std::shared_ptr<indexlib::config::SummaryConfig>> SummaryConfigVec;
    typedef SummaryConfigVec::const_iterator Iterator;
    Iterator Begin() const;
    Iterator End() const;
    size_t GetSummaryFieldsCount() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
