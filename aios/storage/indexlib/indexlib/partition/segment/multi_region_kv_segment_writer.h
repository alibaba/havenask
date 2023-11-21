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
#include "indexlib/indexlib.h"
#include "indexlib/partition/segment/kv_segment_writer.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(document, MultiRegionKVKeyExtractor);

namespace indexlib { namespace partition {

class MultiRegionKVSegmentWriter : public KVSegmentWriter
{
public:
    MultiRegionKVSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                               const config::IndexPartitionOptions& options, uint32_t columnIdx = 0,
                               uint32_t totalColumnCount = 1);

    ~MultiRegionKVSegmentWriter();

public:
    MultiRegionKVSegmentWriter* CloneWithNewSegmentData(index_base::BuildingSegmentData& segmentData,
                                                        bool isShared) const override;

protected:
    void InitConfig() override;
    bool ExtractDocInfo(document::KVIndexDocument* doc, index::keytype_t& key, autil::StringView& value,
                        bool& isDeleted) override;

private:
    document::MultiRegionKVKeyExtractorPtr mKeyExtractor;
    std::vector<int32_t> mFixValueLens;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionKVSegmentWriter);
}} // namespace indexlib::partition
