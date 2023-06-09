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
#include "indexlib/index/kkv/merge/OnDiskKKVIterator.h"

#include "indexlib/index/kkv/merge/OnDiskKKVSegmentIterator.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PoolUtil.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib;
using namespace indexlibv2::config;
using namespace indexlibv2::framework;

namespace indexlibv2::index {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, OnDiskKKVIterator);

template <typename SKeyType>
OnDiskKKVIterator<SKeyType>::OnDiskKKVIterator(const std::shared_ptr<config::KKVIndexConfig>& kkvConfig,
                                               const indexlib::file_system::IOConfig& ioConfig)
    : _ioConfig(ioConfig)
    , _kkvConfig(kkvConfig)
    , _curPkeyIterator(nullptr)
    , _estimatedPkCount(0)
    , _totalProgress(0)
    , _currentProgress(0)
{
}

template <typename SKeyType>
OnDiskKKVIterator<SKeyType>::~OnDiskKKVIterator()
{
    _curPkeySegIterInfo.clear();
    IE_POOL_COMPATIBLE_DELETE_CLASS(&_pool, _curPkeyIterator);
    _curPkeyIterator = nullptr;
    while (!_heap.empty()) {
        OnDiskKKVSegmentIteratorTyped* iter = _heap.top();
        DELETE_AND_SET_NULL(iter);
        _heap.pop();
    }
    for (size_t i = 0; i < _toDeleteSegIterVec.size(); i++) {
        DELETE_AND_SET_NULL(_toDeleteSegIterVec[i]);
    }
}

template <typename SKeyType>
void OnDiskKKVIterator<SKeyType>::Init(const std::vector<IIndexMerger::SourceSegment>& segments)
{
    _totalProgress = 0;
    for (size_t i = 0; i < segments.size(); i++) {
        if (IsEmptySegment(segments[i].segment)) {
            continue;
        }

        OnDiskKKVSegmentIteratorTyped* segIter = new OnDiskKKVSegmentIteratorTyped(i);
        segIter->Init(_kkvConfig, segments[i].segment);
        segIter->ResetBufferParam(_ioConfig.readBufferSize, _ioConfig.enableAsyncRead);
        _segIters.push_back(segIter);
        _totalProgress += segIter->GetPkeyCount();
    }

    if (!CheckSortSequence(_segIters)) {
        for (OnDiskKKVSegmentIteratorTyped* segIter : _segIters) {
            DELETE_AND_SET_NULL(segIter);
        }
        INDEXLIB_FATAL_ERROR(UnSupported, "check sort sequence fail!");
    }

    _estimatedPkCount = DoEstimatePKeyCount(_segIters);

    for (size_t i = 0; i < _segIters.size(); i++) {
        _segIters[i]->Reset();
        PushBackToHeap(_segIters[i]);
    }
    MoveToNext();
}

template <typename SKeyType>
bool OnDiskKKVIterator<SKeyType>::CheckSortSequence(const vector<OnDiskKKVSegmentIteratorTyped*>& segIters)
{
    size_t sortSegmentCount = 0;
    for (size_t i = 0; i < segIters.size(); i++) {
        if (segIters[i]->KeepSortSequence()) {
            sortSegmentCount++;
        }
    }

    if (sortSegmentCount == 0) {
        return true;
    }
    if (sortSegmentCount == 1 && segIters.size() == 1) {
        return true;
    }

    AUTIL_LOG(ERROR, "not support mix more than one sort segment & unsort segment");
    return false;
}

template <typename SKeyType>
size_t OnDiskKKVIterator<SKeyType>::DoEstimatePKeyCount(const vector<OnDiskKKVSegmentIteratorTyped*>& segIters)
{
    bool mergeUsePreciseCount = _kkvConfig->GetIndexPreference().GetHashDictParam().UsePreciseCountWhenMerge();
    size_t count = 0;
    if (!mergeUsePreciseCount) {
        for (size_t i = 0; i < segIters.size(); i++) {
            count += segIters[i]->GetPkeyCount();
        }
        return count;
    }

    SegmentIterHeap heap;
    for (size_t i = 0; i < segIters.size(); i++) {
        segIters[i]->Reset();
        if (segIters[i]->IsValid()) {
            heap.push(segIters[i]);
        }
    }

    uint64_t lastPkey = 0;
    bool isFirstKey = true;
    while (!heap.empty()) {
        OnDiskKKVSegmentIteratorTyped* segIter = heap.top();
        heap.pop();
        uint64_t curPkey = segIter->GetPrefixKey();
        segIter->MoveToNext();
        if (segIter->IsValid()) {
            heap.push(segIter);
        }

        if (isFirstKey) {
            count++;
            lastPkey = curPkey;
            isFirstKey = false;
            continue;
        }

        if (curPkey != lastPkey) {
            count++;
            lastPkey = curPkey;
        }
    }
    return count;
}

template <typename SKeyType>
void OnDiskKKVIterator<SKeyType>::MoveToNext()
{
    _curPkeySegIterInfo.clear();
    IE_POOL_COMPATIBLE_DELETE_CLASS(&_pool, _curPkeyIterator);
    _curPkeyIterator = nullptr;
    if (_pool.getUsedBytes() >= RESET_POOL_THRESHOLD) {
        _pool.reset();
    }
    CreateCurrentPkeyIterator();
}

template <typename SKeyType>
void OnDiskKKVIterator<SKeyType>::CreateCurrentPkeyIterator()
{
    assert(_curPkeySegIterInfo.empty());
    assert(_curPkeyIterator == nullptr);

    bool isFirstKey = true;
    uint64_t firstPkey = 0;
    bool isPkeyDeleted = false;
    uint32_t deletePkeyTs = 0;
    while (!_heap.empty()) {
        ++_currentProgress;
        OnDiskKKVSegmentIteratorTyped* segIter = _heap.top();
        uint64_t curPkey = segIter->GetPrefixKey();
        assert(_kkvConfig);
        if (isFirstKey) {
            firstPkey = curPkey;
        } else if (curPkey != firstPkey) { // different pkey
            break;
        }
        _heap.pop();
        assert(segIter->IsValid());
        if (!isPkeyDeleted) {
            auto singlePkeyIter = segIter->GetIterator(&_pool, _kkvConfig);
            isPkeyDeleted = singlePkeyIter->HasPKeyDeleted();
            deletePkeyTs = singlePkeyIter->GetPKeyDeletedTs();
            if (singlePkeyIter->IsValid()) {
                _curPkeySegIterInfo.push_back(make_pair(segIter->GetSegmentIdx(), singlePkeyIter));
            } else {
                // delete pkey
                IE_POOL_COMPATIBLE_DELETE_CLASS(&_pool, singlePkeyIter);
            }
        }

        segIter->MoveToNext();
        PushBackToHeap(segIter);
        isFirstKey = false;
    }

    if (_curPkeySegIterInfo.empty() && !isPkeyDeleted) {
        return;
    }
    _curPkeyIterator = POOL_COMPATIBLE_NEW_CLASS((&_pool), OnDiskSinglePKeyIteratorTyped, (&_pool), _kkvConfig.get(),
                                                 firstPkey, isPkeyDeleted, deletePkeyTs);
    _curPkeyIterator->Init(_curPkeySegIterInfo);
}

template <typename SKeyType>
bool OnDiskKKVIterator<SKeyType>::IsEmptySegment(const shared_ptr<Segment>& segment)
{
    // DirectoryPtr dataDir = segment.GetIndexDirectory(_kkvConfig->GetIndexName(), false);
    // if (!dataDir) {
    //     AUTIL_LOG(INFO, "segment [%u], shardingIdx [%u] is empty!", segData.GetSegmentId(), segData.GetShardId());
    //     return true;
    // }
    // return !dataDir->IsExist(PREFIX_KEY_FILE_NAME);
    return false;
}

template <typename SKeyType>
void OnDiskKKVIterator<SKeyType>::PushBackToHeap(OnDiskKKVSegmentIteratorTyped* segIter)
{
    if (segIter->IsValid()) {
        _heap.push(segIter);
    } else {
        _toDeleteSegIterVec.push_back(segIter);
    }
}

template <typename SKeyType>
double OnDiskKKVIterator<SKeyType>::GetMergeProgressRatio() const
{
    return _currentProgress < _totalProgress ? (double)(_currentProgress) / _totalProgress : 1.0f;
}

EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(OnDiskKKVIterator);

} // namespace indexlibv2::index
