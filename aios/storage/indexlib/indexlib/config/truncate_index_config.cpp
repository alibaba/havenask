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
#include "indexlib/config/truncate_index_config.h"

#include "indexlib/config/impl/truncate_index_config_impl.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, TruncateIndexConfig);

TruncateIndexConfig::TruncateIndexConfig() : mImpl(new TruncateIndexConfigImpl) {}

TruncateIndexConfig::~TruncateIndexConfig() {}

const string& TruncateIndexConfig::GetIndexName() const { return mImpl->GetIndexName(); }
void TruncateIndexConfig::SetIndexName(const string& indexName) { mImpl->SetIndexName(indexName); }

void TruncateIndexConfig::AddTruncateIndexProperty(const TruncateIndexProperty& property)
{
    mImpl->AddTruncateIndexProperty(property);
}

const TruncateIndexPropertyVector& TruncateIndexConfig::GetTruncateIndexProperties() const
{
    return mImpl->GetTruncateIndexProperties();
}

const TruncateIndexProperty& TruncateIndexConfig::GetTruncateIndexProperty(size_t i) const
{
    return mImpl->GetTruncateIndexProperty(i);
}

TruncateIndexProperty& TruncateIndexConfig::GetTruncateIndexProperty(size_t i)
{
    return mImpl->GetTruncateIndexProperty(i);
}

size_t TruncateIndexConfig::GetTruncateIndexPropertySize() const { return mImpl->GetTruncateIndexPropertySize(); }
}} // namespace indexlib::config
