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
#ifndef __INDEXLIB_INDEX_CUSTOMIZED_INDEX_WRITER_H
#define __INDEXLIB_INDEX_CUSTOMIZED_INDEX_WRITER_H

#include <memory>

#include "autil/ConstString.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/plugin_manager.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, CounterMap);
namespace indexlib { namespace index {

class CustomizedIndexWriter : public IndexWriter
{
public:
    CustomizedIndexWriter(const plugin::PluginManagerPtr& pluginManager, size_t lastSegmentDistinctTermCount,
                          bool ignoreBuildError)
        : mPluginManager(pluginManager)
        , mLastSegDistinctTermCount(lastSegmentDistinctTermCount)
        , mIgnoreBuildError(ignoreBuildError)
    {
    }

    ~CustomizedIndexWriter() {}

public:
    void CustomizedInit(const config::IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics,
                        const index_base::PartitionSegmentIteratorPtr& segIter);
    void Init(const config::IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics) override;

    size_t EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter = index_base::PartitionSegmentIteratorPtr()) override;

    void AddField(const document::Field* field) override;
    void EndDocument(const document::IndexDocument& indexDocument) override;
    void EndSegment() override;
    void Dump(const file_system::DirectoryPtr& indexDir, autil::mem_pool::PoolBase* dumpPool) override;
    std::shared_ptr<IndexSegmentReader> CreateInMemReader() override;

private:
    uint32_t GetDistinctTermCount() const override;
    void UpdateBuildResourceMetrics() override;

    bool GetIndexerSegmentDatas(const config::IndexConfigPtr& indexConfig,
                                const index_base::PartitionSegmentIteratorPtr& segIter,
                                std::vector<IndexerSegmentData>& indexerSegDatas);

private:
    plugin::PluginManagerPtr mPluginManager;
    size_t mLastSegDistinctTermCount;
    IndexerPtr mIndexer;
    std::vector<const document::Field*> mFieldVec;
    bool mIgnoreBuildError;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_CUSTOMIZED_INDEX_WRITER_H
