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
#include "indexlib/partition/segment/multi_region_kkv_segment_writer.h"

#include "indexlib/document/document_parser/kv_parser/multi_region_kkv_keys_extractor.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, MultiRegionKKVSegmentWriter);

MultiRegionKKVSegmentWriter::MultiRegionKKVSegmentWriter(const IndexPartitionSchemaPtr& schema,
                                                         const IndexPartitionOptions& options, uint32_t columnIdx,
                                                         uint32_t totalColumnCount)
    : KKVSegmentWriter<uint64_t>(schema, options, columnIdx, totalColumnCount)
{
}

MultiRegionKKVSegmentWriter::~MultiRegionKKVSegmentWriter() {}

void MultiRegionKKVSegmentWriter::DoInit()
{
    assert(mSchema->GetRegionCount() > 1);
    mKKVConfig = CreateKKVIndexConfigForMultiRegionData(mSchema);
    mMRKeyExtractor.reset(new MultiRegionKKVKeysExtractor(mSchema));
}

bool MultiRegionKKVSegmentWriter::GetKeys(document::KVIndexDocument* doc, PKeyType& pkey, uint64_t& skey,
                                          KKVKeysExtractor::OperationType& operationType)
{
    return mMRKeyExtractor->GetKeys(doc, pkey, skey, operationType);
}

MultiRegionKKVSegmentWriter* MultiRegionKKVSegmentWriter::CloneWithNewSegmentData(BuildingSegmentData& segmentData,
                                                                                  bool isShared) const
{
    assert(isShared);
    MultiRegionKKVSegmentWriter* newSegmentWriter = new MultiRegionKKVSegmentWriter(*this);
    newSegmentWriter->mSegmentData = segmentData;
    return newSegmentWriter;
}
}} // namespace indexlib::partition
