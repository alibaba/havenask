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
#include "indexlib/index/kkv/merge/OnDiskSinglePKeyIterator.h"

#include "indexlib/util/PoolUtil.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlibv2::index {

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, OnDiskKKVSegmentIterator, SKeyType);

template <typename SKeyType>
void OnDiskSinglePKeyIterator<SKeyType>::Init(const vector<SingleSegmentSinglePKeyIterInfo>& multiSegIterInfo)
{
    for (size_t i = 0; i < multiSegIterInfo.size(); i++) {
        assert(multiSegIterInfo[i].second->IsValid());
        _heap.push(multiSegIterInfo[i]);
        _segmentIdxs.push_back(multiSegIterInfo[i].first);
    }
    _segIterInfos = multiSegIterInfo;
    _isValid = !_heap.empty();
    if (Base::IsValid()) {
        FillSKeyInfo();
    }
}

template <typename SKeyType>
void OnDiskSinglePKeyIterator<SKeyType>::MoveToNext()
{
    if (_heap.empty()) {
        _isValid = false;
        return;
    }

    SingleSegmentSinglePKeyIterInfo singleSegIterInfo = _heap.top();
    SKeyType lastSkey = singleSegIterInfo.second->GetCurrentSkey();

    while (true) {
        _heap.pop();
        singleSegIterInfo.second->MoveToNext();
        if (singleSegIterInfo.second->IsValid()) {
            _heap.push(singleSegIterInfo);
        } else {
            indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(Base::_pool, singleSegIterInfo.second);
        }

        if (_heap.empty()) {
            break;
        }

        singleSegIterInfo = _heap.top();
        SKeyType curSkey = singleSegIterInfo.second->GetCurrentSkey();
        if (curSkey != lastSkey) {
            break;
        }
    }
    _isValid = !_heap.empty();
    if (Base::IsValid()) {
        FillSKeyInfo();
    }
}

template <typename SKeyType>
void OnDiskSinglePKeyIterator<SKeyType>::GetCurrentValue(autil::StringView& value)
{
    assert(Base::IsValid());
    const SingleSegmentSinglePKeyIterInfo& singleSegIterInfo = _heap.top();
    singleSegIterInfo.second->GetCurrentValue(value);
}

template <typename SKeyType>
void OnDiskSinglePKeyIterator<SKeyType>::FillSKeyInfo()
{
    const SingleSegmentSinglePKeyIterInfo& singleSegIterInfo = _heap.top();
    const SingleSegmentSinglePKeyIterator* iterator = singleSegIterInfo.second;
    _skey = iterator->GetCurrentSkey();
    _skeyDeleted = iterator->CurrentSkeyDeleted();
    _timestamp = iterator->GetCurrentTs();
    _expireTime = iterator->GetCurrentExpireTime();
}

template <typename SKeyType>
bool OnDiskSinglePKeyIterator<SKeyType>::CurrentSKeyExpired(uint64_t currentTsInSecond)
{
    if (!Base::IsValid()) {
        INDEXLIB_THROW(indexlib::util::InconsistentStateException, "!IsValid");
    }
    if (!(Base::_indexConfig->StoreExpireTime())) {
        return false;
    }
    return Base::SKeyExpired(currentTsInSecond, _timestamp, _expireTime);
}

template <typename SKeyType>
size_t OnDiskSinglePKeyIterator<SKeyType>::GetCurrentSegmentIdx()
{
    assert(Base::IsValid());
    const SingleSegmentSinglePKeyIterInfo& singleSegIterInfo = _heap.top();
    return singleSegIterInfo.first;
}

EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(OnDiskSinglePKeyIterator);

} // namespace indexlibv2::index
