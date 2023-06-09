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
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_writer.h"

#include "autil/EnvUtil.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/index_raw_field.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_module_factory.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_util.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;

using namespace indexlib::document;
using namespace indexlib::common;
using namespace indexlib::plugin;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::util;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, CustomizedIndexWriter);

void CustomizedIndexWriter::Init(const config::IndexConfigPtr& indexConfig,
                                 util::BuildResourceMetrics* buildResourceMetrics)
{
    assert(false);
}

void CustomizedIndexWriter::CustomizedInit(const config::IndexConfigPtr& indexConfig,
                                           util::BuildResourceMetrics* buildResourceMetrics,
                                           const index_base::PartitionSegmentIteratorPtr& segIter)
{
    mIgnoreBuildError = autil::EnvUtil::getEnv("ignore_customize_index_build_error", mIgnoreBuildError);
    IndexWriter::Init(indexConfig, buildResourceMetrics);
    assert(indexConfig->GetShardingType() == config::IndexConfig::IST_NO_SHARDING);
    mIndexer.reset(IndexPluginUtil::CreateIndexer(indexConfig, mPluginManager));
    if (!mIndexer) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "create indexer failed");
    }

    IndexPluginResourcePtr pluginResource =
        DYNAMIC_POINTER_CAST(IndexPluginResource, mPluginManager->GetPluginResource());
    if (!pluginResource) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "empty index plugin resource!");
    }

    IndexerUserDataPtr userData(IndexPluginUtil::CreateIndexerUserData(indexConfig, mPluginManager));
    if (userData) {
        vector<IndexerSegmentData> indexerSegDatas;
        if (!GetIndexerSegmentDatas(indexConfig, segIter, indexerSegDatas)) {
            IE_LOG(WARN, "GetIndexerSegmentData fail!");
            userData.reset();
        }
        if (userData && !userData->init(indexerSegDatas)) {
            IE_LOG(ERROR, "init IndexerUserData fail!");
            userData.reset();
        }
    }
    IndexerResourcePtr resource(new IndexerResource(
        pluginResource->schema, pluginResource->options, pluginResource->counterMap, pluginResource->partitionMeta,
        indexConfig->GetIndexName(), pluginResource->pluginPath, pluginResource->metricProvider, userData));
    if (!mIndexer->Init(resource)) {
        INDEXLIB_FATAL_ERROR(Runtime, "init indexer failed");
    }
    mFieldVec.reserve(indexConfig->GetFieldCount());
    UpdateBuildResourceMetrics();
}

size_t CustomizedIndexWriter::EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                                                 const index_base::PartitionSegmentIteratorPtr& segIter)
{
    size_t userDataInitSize = 0;
    IndexerUserDataPtr userData(IndexPluginUtil::CreateIndexerUserData(indexConfig, mPluginManager));
    if (userData) {
        vector<IndexerSegmentData> indexerSegDatas;
        if (GetIndexerSegmentDatas(indexConfig, segIter, indexerSegDatas)) {
            userDataInitSize = userData->estimateInitMemUse(indexerSegDatas);
        }
    }

    if (mIndexer) {
        return userDataInitSize + mIndexer->EstimateInitMemoryUse(mLastSegDistinctTermCount);
    }
    IndexerPtr tempIndexer(IndexPluginUtil::CreateIndexer(indexConfig, mPluginManager));
    if (tempIndexer) {
        return userDataInitSize + tempIndexer->EstimateInitMemoryUse(mLastSegDistinctTermCount);
    }
    return userDataInitSize;
}

void CustomizedIndexWriter::AddField(const document::Field* field) { mFieldVec.push_back(field); }

void CustomizedIndexWriter::EndDocument(const document::IndexDocument& indexDocument)
{
    if (!mIndexer->Build(mFieldVec, indexDocument.GetDocId())) {
        if (mIgnoreBuildError) {
            IE_LOG(ERROR, "indexer build doc [%u] fail!", indexDocument.GetDocId());
        } else {
            INDEXLIB_FATAL_ERROR(Runtime, "indexer build doc [%u] fail!", indexDocument.GetDocId());
        }
    }
    mFieldVec.clear();
    UpdateBuildResourceMetrics();
}

void CustomizedIndexWriter::EndSegment() { UpdateBuildResourceMetrics(); }

void CustomizedIndexWriter::Dump(const file_system::DirectoryPtr& indexDir, autil::mem_pool::PoolBase* dumpPool)
{
    IE_LOG(DEBUG, "Dumping index : [%s]", _indexConfig->GetIndexName().c_str());
    file_system::DirectoryPtr dumpDir = indexDir->MakeDirectory(_indexConfig->GetIndexName());
    if (!mIndexer->Dump(dumpDir)) {
        INDEXLIB_FATAL_ERROR(Runtime, "indexer dump fail!");
    }
    UpdateBuildResourceMetrics();
}

uint32_t CustomizedIndexWriter::GetDistinctTermCount() const
{
    if (!mIndexer) {
        return 0;
    }
    return mIndexer->GetDistinctTermCount();
}

std::shared_ptr<IndexSegmentReader> CustomizedIndexWriter::CreateInMemReader()
{
    if (!mIndexer) {
        return std::shared_ptr<IndexSegmentReader>();
    }

    InMemSegmentRetrieverPtr segRetriever = mIndexer->CreateInMemSegmentRetriever();
    if (!segRetriever) {
        return std::shared_ptr<IndexSegmentReader>();
    }
    CustomizedIndexSegmentReaderPtr inMemSegReader(new CustomizedIndexSegmentReader);
    if (!inMemSegReader->Init(_indexConfig, segRetriever)) {
        return std::shared_ptr<IndexSegmentReader>();
    }
    return inMemSegReader;
}

void CustomizedIndexWriter::UpdateBuildResourceMetrics()
{
    assert(mIndexer);
    assert(_buildResourceMetricsNode);
    _buildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, mIndexer->EstimateCurrentMemoryUse());
    _buildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, mIndexer->EstimateTempMemoryUseInDump());
    _buildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, mIndexer->EstimateExpandMemoryUseInDump());
    _buildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, mIndexer->EstimateDumpFileSize());
}

bool CustomizedIndexWriter::GetIndexerSegmentDatas(const config::IndexConfigPtr& indexConfig,
                                                   const PartitionSegmentIteratorPtr& partSegIter,
                                                   vector<IndexerSegmentData>& indexerSegDatas)
{
    if (!partSegIter || !indexConfig) {
        return false;
    }
    SegmentIteratorPtr builtSegIter = partSegIter->CreateIterator(SIT_BUILT);
    assert(builtSegIter);
    while (builtSegIter->IsValid()) {
        const SegmentData& segData = builtSegIter->GetSegmentData();
        IndexerSegmentData indexerSegData;
        indexerSegData.segInfo = *(segData.GetSegmentInfo());
        indexerSegData.isBuildingSegment = false;
        indexerSegData.baseDocId = segData.GetBaseDocId();
        indexerSegData.segId = segData.GetSegmentId();
        indexerSegData.indexDir = segData.GetIndexDirectory(indexConfig->GetIndexName(), false);

        indexerSegDatas.push_back(indexerSegData);
        builtSegIter->MoveToNext();
    }
    // TODO: add building indexer segment data
    return true;
}
}} // namespace indexlib::index
