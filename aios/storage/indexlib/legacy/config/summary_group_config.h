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

#include <memory>

#include "autil/Log.h"
#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/index/summary/config/SummaryConfig.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {
class SummaryGroupConfigImpl;
typedef std::shared_ptr<SummaryGroupConfigImpl> SummaryGroupConfigImplPtr;

class SummaryGroupConfig : public indexlibv2::config::SummaryGroupConfig
{
public:
    SummaryGroupConfig(const std::string& groupName = "",
                       index::summarygroupid_t groupId = index::INVALID_SUMMARYGROUPID,
                       index::summaryfieldid_t summaryFieldIdBase = index::INVALID_SUMMARYFIELDID);
    ~SummaryGroupConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    void SetGroupName(const std::string& groupName) override;
    void SetCompress(bool useCompress, const std::string& compressType) override;
    bool IsCompress() const override;
    const std::string& GetCompressType() const override;
    void SetNeedStoreSummary(bool needStoreSummary) override;
    void AddSummaryConfig(const std::shared_ptr<SummaryConfig>& summaryConfig) override;

    void AssertEqual(const SummaryGroupConfig& other) const;
    void AssertCompatible(const SummaryGroupConfig& other) const;
    void SetSummaryGroupDataParam(const GroupDataParameter& param) override;
    void SetEnableAdaptiveOffset(bool enableFlag) override;

public:
    typedef indexlibv2::config::SummaryGroupConfig::SummaryConfigVec SummaryConfigVec;
    typedef indexlibv2::config::SummaryGroupConfig::Iterator Iterator;

private:
    SummaryGroupConfigImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryGroupConfig> SummaryGroupConfigPtr;
}} // namespace indexlib::config
