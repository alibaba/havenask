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
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, JoinDocidAttributeReader);

JoinDocidAttributeReader::JoinDocidAttributeReader() {}

JoinDocidAttributeReader::~JoinDocidAttributeReader() {}

void JoinDocidAttributeReader::InitJoinBaseDocId(const PartitionDataPtr& partitionData)
{
    assert(mSegmentIds.size() == mSegmentReaders.size());
    for (size_t i = 0; i < mSegmentIds.size(); ++i) {
        const SegmentData& segData = partitionData->GetSegmentData(mSegmentIds[i]);
        mJoinedBaseDocIds.push_back(segData.GetBaseDocId());
    }

    SegmentIteratorPtr buildingIter = partitionData->CreateSegmentIterator()->CreateIterator(SIT_BUILDING);
    while (buildingIter->IsValid()) {
        mJoinedBuildingBaseDocIds.push_back(buildingIter->GetBaseDocId());
        buildingIter->MoveToNext();
    }
    assert(!mBuildingAttributeReader || mJoinedBuildingBaseDocIds.size() == mBuildingAttributeReader->Size());
}

bool JoinDocidAttributeReader::Read(docid_t docId, string& attrValue, autil::mem_pool::Pool* pool) const
{
    docid_t joinDocId = GetJoinDocId(docId);
    attrValue = StringUtil::toString<docid_t>(joinDocId);
    return true;
}
}} // namespace indexlib::index
