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
#ifndef __INDEXLIB_RANGE_INDEX_WRITER_H
#define __INDEXLIB_RANGE_INDEX_WRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index, NormalIndexWriter);

namespace indexlib { namespace index {

class RangeInfo;
DEFINE_SHARED_PTR(RangeInfo);

class RangeIndexWriter : public IndexWriter
{
public:
    RangeIndexWriter(const std::string& indexName, std::shared_ptr<framework::SegmentMetrics> segmentMetrics,
                     const config::IndexPartitionOptions& options);
    ~RangeIndexWriter();

public:
    void Init(const config::IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics) override;

    size_t EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter = index_base::PartitionSegmentIteratorPtr()) override;

    void EndDocument(const document::IndexDocument& indexDocument) override;

    void EndSegment() override;

    uint64_t GetNormalTermDfSum() const override
    {
        assert(false);
        return 0;
    }

    void AddField(const document::Field* field) override;

    void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) override;

    IndexSegmentReaderPtr CreateInMemReader() override;
    void FillDistinctTermCount(std::shared_ptr<framework::SegmentMetrics> mSegmentMetrics) override;

    void SetTemperatureLayer(const std::string& layer) override;

private:
    void GetDistinctTermCount(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics,
                              const std::string& indexName, size_t& highLevelTermCount,
                              size_t& bottomLevelTermCount) const;

    uint32_t GetDistinctTermCount() const override
    {
        assert(false);
        return 0;
    }
    void UpdateBuildResourceMetrics() override { assert(false); }

    void InitMemoryPool() override {}

    static constexpr double DEFAULT_BOTTOM_LEVEL_TERM_COUNT_RATIO = 0.8;

public:
    pos_t mBasePos;
    RangeInfoPtr mRangeInfo;
    NormalIndexWriterPtr mBottomLevelWriter;
    NormalIndexWriterPtr mHighLevelWriter;

private:
    friend class RangeIndexWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeIndexWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_RANGE_INDEX_WRITER_H
