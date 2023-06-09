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
#include "indexlib/partition/document_deduper/building_segment_document_deduper.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, BuildingSegmentDocumentDeduper);

void BuildingSegmentDocumentDeduper::Dedup()
{
    IE_LOG(INFO, "begin dedup building segment");
    const IndexConfigPtr& indexConfig = GetPrimaryKeyIndexConfig(mSchema);
    assert(indexConfig);
    InvertedIndexType type = indexConfig->GetInvertedIndexType();
    if (type == it_primarykey64) {
        DedupDocuments<uint64_t>();
    }
    if (type == it_primarykey128) {
        DedupDocuments<autil::uint128_t>();
    }
    assert(type != it_trie);
    IE_LOG(INFO, "end dedup building segment");
}
}} // namespace indexlib::partition
