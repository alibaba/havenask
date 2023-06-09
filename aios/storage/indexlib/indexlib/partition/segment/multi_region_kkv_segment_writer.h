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
#ifndef __INDEXLIB_MULTI_REGION_KKV_SEGMENT_WRITER_H
#define __INDEXLIB_MULTI_REGION_KKV_SEGMENT_WRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/segment/kkv_segment_writer.h"

DECLARE_REFERENCE_CLASS(document, MultiRegionKKVKeysExtractor);
namespace indexlib { namespace partition {

class MultiRegionKKVSegmentWriter : public KKVSegmentWriter<uint64_t>
{
public:
    MultiRegionKKVSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                                const config::IndexPartitionOptions& options, uint32_t columnIdx = 0,
                                uint32_t totalColumnCount = 1);
    ~MultiRegionKKVSegmentWriter();

public:
    MultiRegionKKVSegmentWriter* CloneWithNewSegmentData(index_base::BuildingSegmentData& segmentData,
                                                         bool isShared) const override;

protected:
    void DoInit() override;

    bool GetKeys(document::KVIndexDocument* doc, index::PKeyType& pkey, uint64_t& skey,
                 document::KKVKeysExtractor::OperationType& operationType) override;

private:
    document::MultiRegionKKVKeysExtractorPtr mMRKeyExtractor;
    std::vector<int32_t> mFixValueLens;

private:
    friend class MultiRegionKkvSegmentWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionKKVSegmentWriter);
}} // namespace indexlib::partition

#endif //__INDEXLIB_MULTI_REGION_KKV_SEGMENT_WRITER_H
