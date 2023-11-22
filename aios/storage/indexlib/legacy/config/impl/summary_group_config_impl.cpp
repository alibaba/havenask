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
#include "indexlib/config/impl/summary_group_config_impl.h"

#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, SummaryGroupConfigImpl);

SummaryGroupConfigImpl::SummaryGroupConfigImpl(const std::string& groupName, index::summarygroupid_t groupId)
    : mGroupName(groupName)
    , mGroupId(groupId)
    , mUseCompress(false)
    , mNeedStoreSummary(false)
    , mEnableAdaptiveOffset(false)
{
}

SummaryGroupConfigImpl::~SummaryGroupConfigImpl() {}

void SummaryGroupConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        vector<string> summaryFields;
        summaryFields.reserve(mSummaryConfigs.size());
        for (size_t i = 0; i < mSummaryConfigs.size(); ++i) {
            summaryFields.push_back(mSummaryConfigs[i]->GetSummaryName());
        }
        json.Jsonize(SUMMARY_FIELDS, summaryFields);
        json.Jsonize(SUMMARY_GROUP_NAME, mGroupName);
        json.Jsonize(SUMMARY_COMPRESS, mUseCompress);
        if (mUseCompress && !mCompressType.empty()) {
            json.Jsonize(index::COMPRESS_TYPE, mCompressType);
        }
        json.Jsonize(SUMMARY_GROUP_PARAMTETER, mParameter);
        if (mEnableAdaptiveOffset) {
            json.Jsonize(SUMMARY_ADAPTIVE_OFFSET, mEnableAdaptiveOffset);
        }
    } else {
        assert(false);
    }
}

void SummaryGroupConfigImpl::AddSummaryConfig(const std::shared_ptr<SummaryConfig>& summaryConfig)
{
    mSummaryConfigs.push_back(summaryConfig);
}

void SummaryGroupConfigImpl::SetCompress(bool useCompress, const string& compressType)
{
    mUseCompress = useCompress;
    mCompressType = compressType;
}

bool SummaryGroupConfigImpl::IsCompress() const { return mUseCompress || !mParameter.GetDocCompressor().empty(); }

const std::string& SummaryGroupConfigImpl::GetCompressType() const
{
    if (!mCompressType.empty()) {
        return mCompressType;
    }
    return mParameter.GetDocCompressor();
}

void SummaryGroupConfigImpl::AssertEqual(const SummaryGroupConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mSummaryConfigs.size(), other.mSummaryConfigs.size(), "mSummaryConfigs's size not equal");
    for (size_t i = 0; i < mSummaryConfigs.size(); ++i) {
        auto status = mSummaryConfigs[i]->CheckEqual(*(other.mSummaryConfigs[i]));
        THROW_IF_STATUS_ERROR(status);
    }
    IE_CONFIG_ASSERT_EQUAL(mGroupName, other.mGroupName, "mGroupName not equal");
    IE_CONFIG_ASSERT_EQUAL(mGroupId, other.mGroupId, "mGroupId not equal");
    IE_CONFIG_ASSERT_EQUAL(mEnableAdaptiveOffset, other.mEnableAdaptiveOffset, "mEnableAdaptiveOffset not equal");
    auto status = mParameter.CheckEqual(other.mParameter);
    THROW_IF_STATUS_ERROR(status);
}

void SummaryGroupConfigImpl::AssertCompatible(const SummaryGroupConfigImpl& other) const
{
    IE_CONFIG_ASSERT(mSummaryConfigs.size() <= other.mSummaryConfigs.size(), "mSummaryConfigs' size not compatible");
    for (size_t i = 0; i < mSummaryConfigs.size(); ++i) {
        auto status = mSummaryConfigs[i]->CheckEqual(*(other.mSummaryConfigs[i]));
        THROW_IF_STATUS_ERROR(status);
    }
    IE_CONFIG_ASSERT_EQUAL(mGroupName, other.mGroupName, "mGroupName not equal");
    IE_CONFIG_ASSERT_EQUAL(mGroupId, other.mGroupId, "mGroupId not equal");
}

void SummaryGroupConfigImpl::SetSummaryGroupDataParam(const GroupDataParameter& param) { mParameter = param; }
}} // namespace indexlib::config
