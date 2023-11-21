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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/plugin_manager.h"

namespace indexlib { namespace index {

class IndexPluginResource : public plugin::PluginResource
{
public:
    IndexPluginResource(const config::IndexPartitionSchemaPtr& _schema, const config::IndexPartitionOptions& _options,
                        const util::CounterMapPtr& _counterMap, const index_base::PartitionMeta& _partitionMeta,
                        const std::string& _pluginPath, const util::MetricProviderPtr& _metricProvider)
        : PluginResource(_schema, _options, _counterMap, _pluginPath, _metricProvider)
        , partitionMeta(_partitionMeta)
    {
    }
    ~IndexPluginResource() {}
    index_base::PartitionMeta partitionMeta;
};

DEFINE_SHARED_PTR(IndexPluginResource);
}} // namespace indexlib::index
