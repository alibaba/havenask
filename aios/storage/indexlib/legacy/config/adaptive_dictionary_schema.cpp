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
#include "indexlib/config/adaptive_dictionary_schema.h"

#include "indexlib/config/impl/adaptive_dictionary_schema_impl.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, AdaptiveDictionarySchema);

AdaptiveDictionarySchema::AdaptiveDictionarySchema() { mImpl.reset(new AdaptiveDictionarySchemaImpl()); }
std::shared_ptr<AdaptiveDictionaryConfig>
AdaptiveDictionarySchema::GetAdaptiveDictionaryConfig(const string& ruleName) const
{
    return mImpl->GetAdaptiveDictionaryConfig(ruleName);
}

void AdaptiveDictionarySchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }
void AdaptiveDictionarySchema::AssertEqual(const AdaptiveDictionarySchema& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}
void AdaptiveDictionarySchema::AssertCompatible(const AdaptiveDictionarySchema& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}

void AdaptiveDictionarySchema::AddAdaptiveDictionaryConfig(const std::shared_ptr<AdaptiveDictionaryConfig>& config)
{
    mImpl->AddAdaptiveDictionaryConfig(config);
}
}} // namespace indexlib::config
