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
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class JoinDocidAttributeReader : public SingleValueAttributeReader<docid_t>
{
public:
    JoinDocidAttributeReader();
    virtual ~JoinDocidAttributeReader();

public:
    void InitJoinBaseDocId(const index_base::PartitionDataPtr& partitionData);

    JoinDocidAttributeIterator* CreateIterator(autil::mem_pool::Pool* pool) const override;

    bool Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool = NULL) const override;

public:
    virtual docid_t GetJoinDocId(docid_t docId) const;

protected:
    std::vector<docid_t> mJoinedBaseDocIds;
    std::vector<docid_t> mJoinedBuildingBaseDocIds;

private:
    IE_LOG_DECLARE();
};

inline JoinDocidAttributeIterator* JoinDocidAttributeReader::CreateIterator(autil::mem_pool::Pool* pool) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(pool, JoinDocidAttributeIterator, mSegmentReaders, mBuildingAttributeReader,
                                        mSegmentDocCount, mBuildingBaseDocId, mJoinedBaseDocIds,
                                        mJoinedBuildingBaseDocIds, pool);
}

inline docid_t JoinDocidAttributeReader::GetJoinDocId(docid_t docId) const
{
    docid_t joinDocId = INVALID_DOCID;
    docid_t baseDocId = 0;
    bool isNull = false;
    assert(mJoinedBaseDocIds.size() == mSegmentDocCount.size());
    for (size_t i = 0; i < mSegmentDocCount.size(); i++) {
        docid_t docCount = (docid_t)mSegmentDocCount[i];
        if (docId < baseDocId + docCount) {
            auto ctx = mSegmentReaders[i]->CreateReadContext(nullptr);
            if (!mSegmentReaders[i]->Read(docId - baseDocId, joinDocId, isNull, ctx)) {
                return INVALID_DOCID;
            }
            assert(isNull == false);
            joinDocId += mJoinedBaseDocIds[i];
            return joinDocId;
        }
        baseDocId += docCount;
    }

    if (mBuildingAttributeReader) {
        size_t buildingSegIdx = 0;
        if (mBuildingAttributeReader->Read(docId, joinDocId, buildingSegIdx, isNull)) {
            return joinDocId + mJoinedBuildingBaseDocIds[buildingSegIdx];
        }
    }
    return INVALID_DOCID;
}

DEFINE_SHARED_PTR(JoinDocidAttributeReader);
}} // namespace indexlib::index
