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
#include "indexlib/index/normal/attribute/accessor/section_data_reader.h"

#include "indexlib/config/section_attribute_config.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, SectionDataReader);

SectionDataReader::SectionDataReader() {}

SectionDataReader::~SectionDataReader() {}

void SectionDataReader::InitBuildingAttributeReader(const SegmentIteratorPtr& buildingIter)
{
    if (!buildingIter) {
        return;
    }
    while (buildingIter->IsValid()) {
        index_base::InMemorySegmentPtr inMemorySegment = buildingIter->GetInMemSegment();
        if (!inMemorySegment) {
            buildingIter->MoveToNext();
            continue;
        }
        index_base::InMemorySegmentReaderPtr segmentReader = inMemorySegment->GetSegmentReader();
        if (!segmentReader) {
            buildingIter->MoveToNext();
            continue;
        }

        string indexName = SectionAttributeConfig::SectionAttributeNameToIndexName(mAttrConfig->GetAttrName());
        const auto& indexSegReader = segmentReader->GetMultiFieldIndexSegmentReader()->GetIndexSegmentReader(indexName);
        assert(indexSegReader);
        AttributeSegmentReaderPtr attrSegReader = indexSegReader->GetSectionAttributeSegmentReader();
        assert(attrSegReader);

        InMemSegmentReaderPtr inMemSegReader = DYNAMIC_POINTER_CAST(InMemSegmentReader, attrSegReader);
        if (!mBuildingAttributeReader) {
            mBuildingAttributeReader.reset(new BuildingAttributeReaderType);
        }
        mBuildingAttributeReader->AddSegmentReader(buildingIter->GetBaseDocId(), inMemSegReader);
        buildingIter->MoveToNext();
    }
}
}}} // namespace indexlib::index::legacy
