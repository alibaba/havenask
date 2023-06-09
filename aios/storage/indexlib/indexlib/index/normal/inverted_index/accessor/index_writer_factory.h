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

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(index, IndexWriter);
DECLARE_REFERENCE_CLASS(index, SectionAttributeWriter);

namespace indexlib { namespace index {

class IndexWriterFactory
{
private:
    IndexWriterFactory() = delete;
    ~IndexWriterFactory() = delete;

public:
    static size_t EstimateIndexWriterInitMemUse(const config::IndexConfigPtr& indexConfig,
                                                const config::IndexPartitionOptions& options,
                                                const plugin::PluginManagerPtr& pluginManager,
                                                std::shared_ptr<framework::SegmentMetrics> segmentMetrics,
                                                const index_base::PartitionSegmentIteratorPtr& segIter);

    static std::unique_ptr<index::IndexWriter>
    CreateIndexWriter(segmentid_t segmentId, const config::IndexConfigPtr& indexConfig,
                      std::shared_ptr<framework::SegmentMetrics> segmentMetrics,
                      const config::IndexPartitionOptions& options, util::BuildResourceMetrics* buildResourceMetrics,
                      const plugin::PluginManagerPtr& pluginManager,
                      const index_base::PartitionSegmentIteratorPtr& segIter);

    static bool NeedSectionAttributeWriter(const config::IndexConfigPtr& indexConfig);
    static std::unique_ptr<index::SectionAttributeWriter>
    CreateSectionAttributeWriter(const config::IndexConfigPtr& indexConfig,
                                 util::BuildResourceMetrics* buildResourceMetrics);

private:
    static std::unique_ptr<index::IndexWriter> CreateSingleIndexWriter(
        segmentid_t segmentId, const config::IndexConfigPtr& indexConfig,
        std::shared_ptr<framework::SegmentMetrics> segmentMetrics, const config::IndexPartitionOptions& options,
        util::BuildResourceMetrics* buildResourceMetrics, const plugin::PluginManagerPtr& pluginManager,
        const index_base::PartitionSegmentIteratorPtr& segIter);

private:
    static size_t EstimateSingleIndexWriterInitMemUse(const config::IndexConfigPtr& indexConfig,
                                                      const config::IndexPartitionOptions& options,
                                                      const plugin::PluginManagerPtr& pluginManager,
                                                      std::shared_ptr<framework::SegmentMetrics> segmentMetrics,
                                                      const index_base::PartitionSegmentIteratorPtr& segIter);
    static std::unique_ptr<index::IndexWriter>
    ConstructIndexWriter(segmentid_t segmentId, const config::IndexConfigPtr& indexConfig,
                         std::shared_ptr<framework::SegmentMetrics> segmentMetrics,
                         const config::IndexPartitionOptions& options);

    static std::unique_ptr<index::IndexWriter> CreateIndexWriterWithoutInit(
        segmentid_t segmentId, const config::IndexConfigPtr& indexConfig,
        std::shared_ptr<framework::SegmentMetrics> segmentMetrics, const config::IndexPartitionOptions& options,
        util::BuildResourceMetrics* buildResourceMetrics, const plugin::PluginManagerPtr& pluginManager,
        const index_base::PartitionSegmentIteratorPtr& segIter);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexWriterFactory);
}} // namespace indexlib::index
