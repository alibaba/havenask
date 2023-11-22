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
#include "indexlib/config/dictionary_schema.h"

#include "indexlib/config/impl/dictionary_schema_impl.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, DictionarySchema);

DictionarySchema::DictionarySchema() : mImpl(new DictionarySchemaImpl) {}

DictionarySchema::~DictionarySchema() {}

void DictionarySchema::AddDictionaryConfig(const std::shared_ptr<DictionaryConfig>& dictConfig)
{
    mImpl->AddDictionaryConfig(dictConfig);
}

std::shared_ptr<DictionaryConfig> DictionarySchema::GetDictionaryConfig(const string& dictName) const
{
    return mImpl->GetDictionaryConfig(dictName);
}

void DictionarySchema::Jsonize(Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

void DictionarySchema::AssertEqual(const DictionarySchema& other) const { mImpl->AssertEqual(*(other.mImpl.get())); }

void DictionarySchema::AssertCompatible(const DictionarySchema& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}

DictionarySchema::Iterator DictionarySchema::Begin() const { return mImpl->Begin(); }
DictionarySchema::Iterator DictionarySchema::End() const { return mImpl->End(); }
}} // namespace indexlib::config
