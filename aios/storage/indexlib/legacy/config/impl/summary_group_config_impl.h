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
#include "indexlib/config/summary_group_config.h"
#include "indexlib/index/summary/config/SummaryConfig.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class SummaryGroupConfigImpl : public autil::legacy::Jsonizable
{
public:
    SummaryGroupConfigImpl(const std::string& groupName = "",
                           index::summarygroupid_t groupId = index::INVALID_SUMMARYGROUPID);
    ~SummaryGroupConfigImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    void SetGroupName(const std::string& groupName) { mGroupName = groupName; }
    void SetCompress(bool useCompress, const std::string& compressType);
    bool IsCompress() const;
    const std::string& GetCompressType() const;
    void SetNeedStoreSummary(bool needStoreSummary) { mNeedStoreSummary = needStoreSummary; }
    void AddSummaryConfig(const std::shared_ptr<SummaryConfig>& summaryConfig);

    void AssertEqual(const SummaryGroupConfigImpl& other) const;
    void AssertCompatible(const SummaryGroupConfigImpl& other) const;

    void SetSummaryGroupDataParam(const GroupDataParameter& param);
    void SetEnableAdaptiveOffset(bool enableFlag) { mEnableAdaptiveOffset = enableFlag; }

public:
    typedef std::vector<std::shared_ptr<SummaryConfig>> SummaryConfigVec;

private:
    SummaryConfigVec mSummaryConfigs;
    std::string mGroupName;
    std::string mCompressType; // TODO: deleted, use mParameter instead
    GroupDataParameter mParameter;
    index::summarygroupid_t mGroupId;
    bool mUseCompress;      // TODO: deleted, use mParameter instead
    bool mNeedStoreSummary; // true: if all fields in attributes
    bool mEnableAdaptiveOffset;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryGroupConfigImpl> SummaryGroupConfigImplPtr;
}} // namespace indexlib::config
