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
#include "indexlib/config/truncate_profile_schema.h"

#include "indexlib/config/impl/truncate_profile_schema_impl.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, TruncateProfileSchema);

TruncateProfileSchema::TruncateProfileSchema() { mImpl.reset(new TruncateProfileSchemaImpl()); }

TruncateProfileConfigPtr TruncateProfileSchema::GetTruncateProfileConfig(const string& truncateProfileName) const
{
    return mImpl->GetTruncateProfileConfig(truncateProfileName);
}
void TruncateProfileSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

void TruncateProfileSchema::AssertEqual(const TruncateProfileSchema& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

TruncateProfileSchema::Iterator TruncateProfileSchema::Begin() const { return mImpl->Begin(); }
TruncateProfileSchema::Iterator TruncateProfileSchema::End() const { return mImpl->End(); }

size_t TruncateProfileSchema::Size() const { return mImpl->Size(); }

const TruncateProfileSchema::TruncateProfileConfigMap& TruncateProfileSchema::GetTruncateProfileConfigMap()
{
    return mImpl->GetTruncateProfileConfigMap();
}

void TruncateProfileSchema::AddTruncateProfileConfig(const TruncateProfileConfigPtr& truncateProfileConfig)
{
    mImpl->AddTruncateProfileConfig(truncateProfileConfig);
}
}} // namespace indexlib::config
