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
#ifndef __INDEXLIB_COMBINE_SEGMENTS_PRIMARY_KEY_ITERATOR_H
#define __INDEXLIB_COMBINE_SEGMENTS_PRIMARY_KEY_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/primarykey/primary_key_hash_table.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator_creator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class CombineSegmentsPrimaryKeyIterator : public PrimaryKeyIterator<Key>
{
private:
    typedef PKPair<Key> TypedPKPair;
    typedef std::shared_ptr<PrimaryKeyIterator<Key>> PrimaryKeyIteratorPtr;

public:
    CombineSegmentsPrimaryKeyIterator(const config::PrimaryKeyIndexConfigPtr& pkConfig);
    ~CombineSegmentsPrimaryKeyIterator();

public:
    void Init(const std::vector<index_base::SegmentData>& segmentDatas) override;
    uint64_t GetPkCount() const override;
    uint64_t GetDocCount() const override;
    bool HasNext() const override;
    void Next(TypedPKPair& pkPair) override;

private:
    PrimaryKeyIteratorPtr mPrimaryKeyIterator;
    docid_t mBaseDocId;

private:
    IE_LOG_DECLARE();
};

template <typename Key>
CombineSegmentsPrimaryKeyIterator<Key>::CombineSegmentsPrimaryKeyIterator(
    const config::PrimaryKeyIndexConfigPtr& pkConfig)
    : PrimaryKeyIterator<Key>(pkConfig)
    , mBaseDocId(0)
{
    mPrimaryKeyIterator = PrimaryKeyIteratorCreator::Create<Key>(pkConfig);
}

template <typename Key>
CombineSegmentsPrimaryKeyIterator<Key>::~CombineSegmentsPrimaryKeyIterator()
{
}

template <typename Key>
void CombineSegmentsPrimaryKeyIterator<Key>::Init(const std::vector<index_base::SegmentData>& segmentDatas)
{
    mPrimaryKeyIterator->Init(segmentDatas);
    if (segmentDatas.size() > 0) {
        mBaseDocId = segmentDatas[0].GetBaseDocId();
    } else {
        mBaseDocId = 0;
    }
}

template <typename Key>
uint64_t CombineSegmentsPrimaryKeyIterator<Key>::GetPkCount() const
{
    return mPrimaryKeyIterator->GetPkCount();
}

template <typename Key>
uint64_t CombineSegmentsPrimaryKeyIterator<Key>::GetDocCount() const
{
    return mPrimaryKeyIterator->GetDocCount();
}

template <typename Key>
bool CombineSegmentsPrimaryKeyIterator<Key>::HasNext() const
{
    return mPrimaryKeyIterator->HasNext();
}

template <typename Key>
void CombineSegmentsPrimaryKeyIterator<Key>::Next(TypedPKPair& pkPair)
{
    mPrimaryKeyIterator->Next(pkPair);
    pkPair.docid -= mBaseDocId;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_COMBINE_SEGMENTS_PRIMARY_KEY_ITERATOR_H
