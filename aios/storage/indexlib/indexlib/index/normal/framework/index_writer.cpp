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
#include "indexlib/index/normal/framework/index_writer.h"

#include "indexlib/config/index_config.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/util/build_profiling_metrics.h"
#include "indexlib/index_define.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace {
class ScopeBuildProfilingReporter
{
public:
    ScopeBuildProfilingReporter(indexid_t id, const indexlib::index::BuildProfilingMetricsPtr& metric)
        : _beginTime(0)
        , _id(id)
        , _metric(metric)
    {
        if (_metric) {
            _beginTime = autil::TimeUtility::currentTime();
        }
    }

    ~ScopeBuildProfilingReporter()
    {
        if (_metric) {
            int64_t endTime = autil::TimeUtility::currentTime();
            _metric->CollectAddSingleIndexTime(_id, endTime - _beginTime);
        }
    }

private:
    int64_t _beginTime;
    indexid_t _id;
    const indexlib::index::BuildProfilingMetricsPtr& _metric;
};
} // namespace

namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexWriter);

void IndexWriter::FillDistinctTermCount(std::shared_ptr<indexlib::framework::SegmentMetrics> segmentMetrics)
{
    segmentMetrics->SetDistinctTermCount(_indexName, GetDistinctTermCount());
}

void IndexWriter::Init(const IndexConfigPtr& indexConfig, BuildResourceMetrics* buildResourceMetrics)
{
    _indexConfig = indexConfig;
    _indexName = InvertedIndexUtil::GetIndexName(_indexConfig);

    InitMemoryPool();
    if (buildResourceMetrics) {
        _buildResourceMetricsNode = buildResourceMetrics->AllocateNode();
        assert(indexConfig);
        IE_LOG(INFO, "allocate build resource node [id:%d] for index[%s]", _buildResourceMetricsNode->GetNodeId(),
               _indexName.c_str());
    }
}

void IndexWriter::InitMemoryPool()
{
    _allocator.reset(new MMapAllocator);
    _byteSlicePool.reset(new Pool(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024));
}

void IndexWriter::AddField(const BuildProfilingMetricsPtr& metrics, const document::Field* field)
{
    indexid_t id = _indexConfig ? _indexConfig->GetIndexId() : INVALID_INDEXID;
    ScopeBuildProfilingReporter reporter(id, metrics);
    return AddField(field);
}

void IndexWriter::EndDocument(const BuildProfilingMetricsPtr& metrics, const document::IndexDocument& indexDocument)
{
    indexid_t id = _indexConfig ? _indexConfig->GetIndexId() : INVALID_INDEXID;
    ScopeBuildProfilingReporter reporter(id, metrics);
    return EndDocument(indexDocument);
}
}} // namespace indexlib::index
