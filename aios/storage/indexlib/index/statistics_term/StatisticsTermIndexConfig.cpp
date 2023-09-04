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
#include "indexlib/index/statistics_term/StatisticsTermIndexConfig.h"

#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/statistics_term/Constant.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.config, StatisticsTermIndexConfig);

struct StatisticsTermIndexConfig::Impl {
    std::string indexName;
    std::vector<std::string> associatedInvertedIndexNames;
};

StatisticsTermIndexConfig::StatisticsTermIndexConfig() : _impl(std::make_unique<StatisticsTermIndexConfig::Impl>()) {}
StatisticsTermIndexConfig::~StatisticsTermIndexConfig() {}

const std::string& StatisticsTermIndexConfig::GetIndexType() const { return index::STATISTICS_TERM_INDEX_TYPE_STR; }

const std::string& StatisticsTermIndexConfig::GetIndexName() const { return _impl->indexName; }

const std::string& StatisticsTermIndexConfig::GetIndexCommonPath() const { return index::STATISTICS_TERM_INDEX_PATH; }

std::vector<std::string> StatisticsTermIndexConfig::GetIndexPath() const
{
    return {GetIndexCommonPath() + "/" + GetIndexName()};
}

std::vector<std::string> StatisticsTermIndexConfig::GetInvertedIndexNames() const
{
    return _impl->associatedInvertedIndexNames;
}

void StatisticsTermIndexConfig::Check() const {}

void StatisticsTermIndexConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                            const config::IndexConfigDeserializeResource& resource)
{
    autil::legacy::Jsonizable::JsonWrapper wrapper(any);
    Jsonize(wrapper);
}

void StatisticsTermIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    StatisticsTermIndexConfig* self = const_cast<StatisticsTermIndexConfig*>(this);
    self->Jsonize(json);
}

void StatisticsTermIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("index_name", _impl->indexName);
    json.Jsonize("associated_inverted_index", _impl->associatedInvertedIndexNames);
}
Status StatisticsTermIndexConfig::CheckCompatible(const IIndexConfig* other) const { return Status::OK(); }

std::vector<std::shared_ptr<config::FieldConfig>> StatisticsTermIndexConfig::GetFieldConfigs() const { return {}; }

bool StatisticsTermIndexConfig::IsDisabled() const { return false; }
} // namespace indexlibv2::index
