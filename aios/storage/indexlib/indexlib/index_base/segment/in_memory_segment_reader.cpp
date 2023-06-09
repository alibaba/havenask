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
#include "indexlib/index_base/segment/in_memory_segment_reader.h"

using namespace std;

using namespace indexlib::index;
namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, InMemorySegmentReader);

InMemorySegmentReader::InMemorySegmentReader(segmentid_t segId) : mSegmentId(segId) {}

InMemorySegmentReader::~InMemorySegmentReader() {}

void InMemorySegmentReader::Init(const KKVSegmentReaderPtr& buildingKKVSegmentReader, const SegmentInfo& segmentInfo)
{
    mBuildingKKVSegmentReader = buildingKKVSegmentReader;
    mSegmentInfo = segmentInfo;
}
void InMemorySegmentReader::Init(const MultiFieldIndexSegmentReaderPtr& indexSegmentReader,
                                 const String2AttrReaderMap& attrReaders, const SummarySegmentReaderPtr& summaryReader,
                                 const InMemSourceSegmentReaderPtr& sourceReader,
                                 const InMemDeletionMapReaderPtr& delMap, const SegmentInfo& segmentInfo)
{
    mIndexSegmentReader = indexSegmentReader;
    mAttrReaders = attrReaders;
    mSummaryReader = summaryReader;
    mSourceReader = sourceReader;
    mInMemDeletionMapReader = delMap;
    mSegmentInfo = segmentInfo;
}

AttributeSegmentReaderPtr InMemorySegmentReader::GetAttributeSegmentReader(const string& name) const
{
    String2AttrReaderMap::const_iterator it = mAttrReaders.find(name);
    if (it != mAttrReaders.end()) {
        return it->second;
    }
    return AttributeSegmentReaderPtr();
}

SummarySegmentReaderPtr InMemorySegmentReader::GetSummaryReader() const { return mSummaryReader; }

InMemDeletionMapReaderPtr InMemorySegmentReader::GetInMemDeletionMapReader() const { return mInMemDeletionMapReader; }
}} // namespace indexlib::index_base
