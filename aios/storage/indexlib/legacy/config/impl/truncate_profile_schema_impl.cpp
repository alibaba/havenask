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
#include "indexlib/config/impl/truncate_profile_schema_impl.h"

#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, TruncateProfileSchemaImpl);

TruncateProfileSchemaImpl::TruncateProfileSchemaImpl() {}

TruncateProfileSchemaImpl::~TruncateProfileSchemaImpl() {}

TruncateProfileConfigPtr TruncateProfileSchemaImpl::GetTruncateProfileConfig(const string& truncateProfileName) const
{
    Iterator it = mTruncateProfileConfigs.find(truncateProfileName);
    if (it != mTruncateProfileConfigs.end()) {
        return it->second;
    }
    return TruncateProfileConfigPtr();
}

void TruncateProfileSchemaImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        Iterator it = mTruncateProfileConfigs.begin();
        vector<Any> anyVec;
        anyVec.reserve(mTruncateProfileConfigs.size());
        for (; it != mTruncateProfileConfigs.end(); ++it) {
            anyVec.push_back(ToJson(*(it->second)));
        }
        json.Jsonize(TRUNCATE_PROFILES, anyVec);
    } else {
        std::vector<TruncateProfileConfig> truncateProfileConfigs;
        json.Jsonize(TRUNCATE_PROFILES, truncateProfileConfigs, truncateProfileConfigs);
        std::vector<TruncateProfileConfig>::const_iterator it = truncateProfileConfigs.begin();
        for (; it != truncateProfileConfigs.end(); ++it) {
            it->Check();
            mTruncateProfileConfigs[it->GetTruncateProfileName()] =
                TruncateProfileConfigPtr(new TruncateProfileConfig(*it));
        }
    }
}

void TruncateProfileSchemaImpl::AssertEqual(const TruncateProfileSchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mTruncateProfileConfigs.size(), other.mTruncateProfileConfigs.size(),
                           "mTruncateProfileConfigs' size not equal");

    Iterator it = mTruncateProfileConfigs.begin();
    for (; it != mTruncateProfileConfigs.end(); ++it) {
        it->second->AssertEqual(*(other.GetTruncateProfileConfig(it->first)));
    }
}

void TruncateProfileSchemaImpl::AddTruncateProfileConfig(const TruncateProfileConfigPtr& truncateProfileConfig)
{
    Iterator it = mTruncateProfileConfigs.find(truncateProfileConfig->GetTruncateProfileName());
    if (it != mTruncateProfileConfigs.end()) {
        // already exist
        return;
    }
    mTruncateProfileConfigs[truncateProfileConfig->GetTruncateProfileName()] = truncateProfileConfig;
}
}} // namespace indexlib::config
