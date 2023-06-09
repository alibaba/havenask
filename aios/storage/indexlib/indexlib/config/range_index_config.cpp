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
#include "indexlib/config/range_index_config.h"

#include "indexlib/index/inverted_index/config/RangeIndexConfig.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, RangeIndexConfig);

struct RangeIndexConfig::Impl {
    IndexConfigPtr bottomLevelConfig;
    IndexConfigPtr highLevelConfig;
};

RangeIndexConfig::RangeIndexConfig(const string& indexName, InvertedIndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , mImpl(make_unique<Impl>())
{
}

RangeIndexConfig::RangeIndexConfig(const RangeIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , mImpl(make_unique<Impl>(*(other.mImpl)))
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

IndexConfig* RangeIndexConfig::Clone() const { return new RangeIndexConfig(*this); }

std::unique_ptr<indexlibv2::config::InvertedIndexConfig> RangeIndexConfig::ConstructConfigV2() const
{
    return DoConstructConfigV2<indexlibv2::config::RangeIndexConfig>();
}

string RangeIndexConfig::GetBottomLevelIndexName(const std::string& indexName) { return indexName + "_@_bottom"; }

string RangeIndexConfig::GetHighLevelIndexName(const std::string& indexName) { return indexName + "_@_high"; }

IndexConfigPtr RangeIndexConfig::GetBottomLevelIndexConfig()
{
    if (!mImpl->bottomLevelConfig) {
        mImpl->bottomLevelConfig = make_shared<RangeIndexConfig>(*this);
        mImpl->bottomLevelConfig->SetIndexName(RangeIndexConfig::GetBottomLevelIndexName(GetIndexName()));
        mImpl->bottomLevelConfig->SetFileCompressConfig(GetFileCompressConfig());
    }
    return mImpl->bottomLevelConfig;
}

IndexConfigPtr RangeIndexConfig::GetHighLevelIndexConfig()
{
    if (!mImpl->highLevelConfig) {
        mImpl->highLevelConfig = make_shared<RangeIndexConfig>(*this);
        mImpl->highLevelConfig->SetIndexName(RangeIndexConfig::GetHighLevelIndexName(GetIndexName()));
        mImpl->highLevelConfig->SetFileCompressConfig(GetFileCompressConfig());
    }
    return mImpl->highLevelConfig;
}

bool RangeIndexConfig::CheckFieldType(FieldType ft) const
{
    if (!SingleFieldIndexConfig::CheckFieldType(ft)) {
        return false;
    }
    if (ft == ft_uint64) {
        IE_LOG(ERROR, "range index not support uint64 type");
        return false;
    }
    return true;
}
}} // namespace indexlib::config
