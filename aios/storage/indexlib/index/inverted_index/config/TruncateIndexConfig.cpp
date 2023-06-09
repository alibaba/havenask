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
#include "indexlib/index/inverted_index/config/TruncateIndexConfig.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, TruncateIndexConfig);

struct TruncateIndexConfig::Impl {
    std::string indexName;
    std::vector<TruncateIndexProperty> truncIndexProperties;
};

TruncateIndexConfig::TruncateIndexConfig() : _impl(std::make_unique<TruncateIndexConfig::Impl>()) {}
TruncateIndexConfig::~TruncateIndexConfig() {}

const std::string& TruncateIndexConfig::GetIndexName() const { return _impl->indexName; }
void TruncateIndexConfig::SetIndexName(const std::string& name) { _impl->indexName = name; }

void TruncateIndexConfig::AddTruncateIndexProperty(const TruncateIndexProperty& property)
{
    _impl->truncIndexProperties.push_back(property);
}

const std::vector<TruncateIndexProperty>& TruncateIndexConfig::GetTruncateIndexProperties() const
{
    return _impl->truncIndexProperties;
}

const TruncateIndexProperty& TruncateIndexConfig::GetTruncateIndexProperty(size_t i) const
{
    assert(i < _impl->truncIndexProperties.size());
    return _impl->truncIndexProperties[i];
}

size_t TruncateIndexConfig::GetTruncateIndexPropertySize() const { return _impl->truncIndexProperties.size(); }

} // namespace indexlibv2::config
