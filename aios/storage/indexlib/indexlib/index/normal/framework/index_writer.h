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

#include <assert.h>
#include <memory>
#include <queue>
#include <string>

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/base/Status.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"

DECLARE_REFERENCE_CLASS(document, Field);
DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(document, ModifiedTokens);
DECLARE_REFERENCE_CLASS(index, IndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, BuildProfilingMetrics);
DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, MMapAllocator);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetricsNode);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

namespace indexlib { namespace index {

class IndexWriter
{
public:
    IndexWriter() : _buildResourceMetricsNode(nullptr) {}

    virtual ~IndexWriter() {}

public:
    virtual void Init(const config::IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics);

    virtual size_t EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter = index_base::PartitionSegmentIteratorPtr()) = 0;

    virtual void AddField(const document::Field* field) = 0;
    virtual void UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens) {}
    virtual void UpdateField(docid_t docId, index::DictKeyInfo termKey, bool isDelete) {}
    virtual void EndDocument(const document::IndexDocument& indexDocument) = 0;
    virtual void EndSegment() = 0;

    virtual void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) = 0;

    virtual const config::IndexConfigPtr& GetIndexConfig() const { return _indexConfig; }

    virtual std::shared_ptr<index::IndexSegmentReader> CreateInMemReader() = 0;

    // for test
    virtual uint64_t GetNormalTermDfSum() const
    {
        assert(false);
        return 0;
    }
    virtual void GetDumpEstimateFactor(std::priority_queue<double>& factors,
                                       std::priority_queue<size_t>& minMemUses) const
    {
    }
    virtual void FillDistinctTermCount(std::shared_ptr<framework::SegmentMetrics> mSegmentMetrics);

    void AddField(const BuildProfilingMetricsPtr& metrics, const document::Field* field);
    void EndDocument(const BuildProfilingMetricsPtr& metrics, const document::IndexDocument& indexDocument);
    virtual void SetTemperatureLayer(const std::string& layer) { _temperatureLayer = layer; }
    const std::string& GetTemperatureLayer() const { return _temperatureLayer; }

private:
    virtual uint32_t GetDistinctTermCount() const = 0;

protected:
    typedef std::shared_ptr<autil::mem_pool::Pool> PoolPtr;

protected:
    virtual void InitMemoryPool();
    virtual void UpdateBuildResourceMetrics() = 0;

protected:
    util::BuildResourceMetricsNode* _buildResourceMetricsNode;
    config::IndexConfigPtr _indexConfig;
    std::string _indexName;
    util::MMapAllocatorPtr _allocator;
    PoolPtr _byteSlicePool;
    std::string _temperatureLayer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexWriter);
}} // namespace indexlib::index
