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
#include "indexlib/index/kkv/on_disk_single_pkey_iterator.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OnDiskSinglePKeyIterator);

void OnDiskSinglePKeyIterator::Init(const vector<SingleSegmentSinglePKeyIterInfo>& multiSegIterInfo)
{
    for (size_t i = 0; i < multiSegIterInfo.size(); i++) {
        assert(multiSegIterInfo[i].second->IsValid());
        mHeap.push(multiSegIterInfo[i]);
        mSegmentIdxs.push_back(multiSegIterInfo[i].first);
    }
    mSegIterInfos = multiSegIterInfo;
}

bool OnDiskSinglePKeyIterator::MoveToNext()
{
    if (mHeap.empty()) {
        return true;
    }

    uint64_t lastSkey = 0;
    bool isDeleted = false;
    SingleSegmentSinglePKeyIterInfo singleSegIterInfo = mHeap.top();
    singleSegIterInfo.second->GetCurrentSkey(lastSkey, isDeleted);

    while (true) {
        mHeap.pop();
        singleSegIterInfo.second->MoveToNext();
        if (singleSegIterInfo.second->IsValid()) {
            mHeap.push(singleSegIterInfo);
        } else {
            POOL_COMPATIBLE_DELETE_CLASS(mPool, singleSegIterInfo.second);
        }

        if (mHeap.empty()) {
            break;
        }

        uint64_t curSkey = 0;
        singleSegIterInfo = mHeap.top();
        singleSegIterInfo.second->GetCurrentSkey(curSkey, isDeleted);
        if (curSkey != lastSkey) {
            break;
        }
    }
    return true;
}

void OnDiskSinglePKeyIterator::GetCurrentSkey(uint64_t& skey, bool& isDeleted) const
{
    assert(IsValid());
    const SingleSegmentSinglePKeyIterInfo& singleSegIterInfo = mHeap.top();
    return singleSegIterInfo.second->GetCurrentSkey(skey, isDeleted);
}

void OnDiskSinglePKeyIterator::GetCurrentValue(StringView& value)
{
    assert(IsValid());
    const SingleSegmentSinglePKeyIterInfo& singleSegIterInfo = mHeap.top();
    return singleSegIterInfo.second->GetCurrentValue(value);
}

uint32_t OnDiskSinglePKeyIterator::GetCurrentTs()
{
    assert(IsValid());
    const SingleSegmentSinglePKeyIterInfo& singleSegIterInfo = mHeap.top();
    return singleSegIterInfo.second->GetCurrentTs();
}

uint32_t OnDiskSinglePKeyIterator::GetCurrentExpireTime()
{
    assert(IsValid());
    const SingleSegmentSinglePKeyIterInfo& singleSegIterInfo = mHeap.top();
    return singleSegIterInfo.second->GetCurrentExpireTime();
}

bool OnDiskSinglePKeyIterator::CurrentSKeyExpired(uint64_t currentTsInSecond)
{
    if (!IsValid()) {
        INDEXLIB_THROW(util::InconsistentStateException, "!IsValid");
    }
    assert(IsValid());
    const SingleSegmentSinglePKeyIterInfo& singleSegIterInfo = mHeap.top();
    return singleSegIterInfo.second->CurrentSKeyExpired(currentTsInSecond);
}

size_t OnDiskSinglePKeyIterator::GetCurrentSegmentIdx()
{
    assert(IsValid());
    const SingleSegmentSinglePKeyIterInfo& singleSegIterInfo = mHeap.top();
    return singleSegIterInfo.first;
}

}} // namespace indexlib::index
