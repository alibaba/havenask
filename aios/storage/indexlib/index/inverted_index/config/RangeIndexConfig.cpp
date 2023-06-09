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
#include "indexlib/index/inverted_index/config/RangeIndexConfig.h"

#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, RangeIndexConfig);

struct RangeIndexConfig::Impl {
    std::shared_ptr<InvertedIndexConfig> bottomLevelConfig;
    std::shared_ptr<InvertedIndexConfig> highLevelConfig;
};

RangeIndexConfig::RangeIndexConfig(const string& indexName, InvertedIndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , _impl(make_unique<Impl>())
{
}

RangeIndexConfig::RangeIndexConfig(const RangeIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , _impl(make_unique<Impl>(*(other._impl)))
{
}

RangeIndexConfig::~RangeIndexConfig() {}

void RangeIndexConfig::Check() const
{
    SingleFieldIndexConfig::Check();
    if (IsHashTypedDictionary()) {
        INDEXLIB_FATAL_ERROR(Schema, "range index should not set use_hash_typed_dictionary");
    }
}

InvertedIndexConfig* RangeIndexConfig::Clone() const { return new RangeIndexConfig(*this); }

string RangeIndexConfig::GetBottomLevelIndexName(const std::string& indexName) { return indexName + "_@_bottom"; }

string RangeIndexConfig::GetHighLevelIndexName(const std::string& indexName) { return indexName + "_@_high"; }

std::shared_ptr<InvertedIndexConfig> RangeIndexConfig::GetBottomLevelIndexConfig()
{
    if (!_impl->bottomLevelConfig) {
        _impl->bottomLevelConfig = make_shared<RangeIndexConfig>(*this);
        _impl->bottomLevelConfig->SetIndexName(RangeIndexConfig::GetBottomLevelIndexName(GetIndexName()));
        _impl->bottomLevelConfig->SetFileCompressConfig(GetFileCompressConfig());
    }
    return _impl->bottomLevelConfig;
}

std::shared_ptr<InvertedIndexConfig> RangeIndexConfig::GetHighLevelIndexConfig()
{
    if (!_impl->highLevelConfig) {
        _impl->highLevelConfig = make_shared<RangeIndexConfig>(*this);
        _impl->highLevelConfig->SetIndexName(RangeIndexConfig::GetHighLevelIndexName(GetIndexName()));
        _impl->highLevelConfig->SetFileCompressConfig(GetFileCompressConfig());
    }
    return _impl->highLevelConfig;
}

bool RangeIndexConfig::CheckFieldType(FieldType ft) const
{
    if (!SingleFieldIndexConfig::CheckFieldType(ft)) {
        return false;
    }
    if (ft == ft_uint64) {
        AUTIL_LOG(ERROR, "range index not support uint64 type");
        return false;
    }
    return true;
}

} // namespace indexlibv2::config
