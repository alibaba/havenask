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

#include "indexlib/framework/Locator.h"
#include "indexlib/index/kkv/building/KKVBuildingSegmentReader.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentReader.h"
#include "indexlib/index/kkv/common/SKeySearchContext.h"
#include "indexlib/index/kkv/search/KKVIteratorImplBase.h"
namespace indexlibv2::index {
template <typename SKeyType>
class NormalKKVIteratorImpl final : public KKVIteratorImplBase
{
private:
    using LocatorPtr = std::shared_ptr<indexlibv2::framework::Locator>;
    using MemShardReaderAndLocator = std::pair<std::shared_ptr<KKVBuildingSegmentReader<SKeyType>>, LocatorPtr>;
    using DiskShardReaderAndLocator = std::pair<std::shared_ptr<KKVBuiltSegmentReader<SKeyType>>, LocatorPtr>;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;

public:
    template <typename Alloc>
    NormalKKVIteratorImpl(autil::mem_pool::Pool* pool, config::KKVIndexConfig* indexConfig, uint64_t ttl, PKeyType pkey,
                          const std::vector<uint64_t, Alloc>& skeyHashVec,
                          const std::vector<DiskShardReaderAndLocator>& builtSegReaders,
                          const std::vector<MemShardReaderAndLocator>& buildingSegReaders, uint64_t curTimeInSecond,
                          KKVMetricsCollector* metricsCollector = nullptr);
    ~NormalKKVIteratorImpl();

public:
    void BatchGet(KKVResultBuffer& resultBuffer) override;

private:
    bool SwitchIterator();
    bool SwitchBuildingSegmentIterator();
    bool SwitchBuiltSegmentIterator();
    bool IsLastBuiltSegment() const;
    bool IsObseleteBuiltSegment(int32_t segIdx) const;

private:
    const std::vector<DiskShardReaderAndLocator>& _builtSegReaders;
    const std::vector<MemShardReaderAndLocator>& _buildingSegReaders;
    bool _hasOnlySegment = false;

    KKVSegmentIteratorBase<SKeyType>* _curSegIterator = nullptr;
    int32_t _curBuildingSegIdx = -1;
    int32_t _curBuiltSegIdx = 0;
    bool _searchingBuildingSeg = false;

    SKeyContext* _skeyContext = nullptr;
    PooledSKeySet _foundSKeys;
    uint32_t _curTsInSecond = 0;
    uint32_t _minTsInSecond = 0;
};

template <typename SKeyType>
template <typename Alloc>
NormalKKVIteratorImpl<SKeyType>::NormalKKVIteratorImpl(autil::mem_pool::Pool* pool, config::KKVIndexConfig* indexConfig,
                                                       uint64_t ttl, PKeyType pkey,
                                                       const std::vector<uint64_t, Alloc>& skeyHashVec,
                                                       const std::vector<DiskShardReaderAndLocator>& builtSegReaders,
                                                       const std::vector<MemShardReaderAndLocator>& buildingSegReaders,
                                                       uint64_t curTimeInSecond, KKVMetricsCollector* metricsCollector)
    : KKVIteratorImplBase(pool, pkey, indexConfig, metricsCollector)
    , _builtSegReaders(builtSegReaders)
    , _buildingSegReaders(buildingSegReaders)
    , _curBuiltSegIdx(static_cast<int32_t>(builtSegReaders.size()))
    , _foundSKeys(autil::mem_pool::pool_allocator<SKeyType>(_pool))
    , _curTsInSecond(static_cast<uint32_t>(curTimeInSecond))
{
    _hasOnlySegment = (_buildingSegReaders.size() + _builtSegReaders.size()) == 1;
    _searchingBuildingSeg = buildingSegReaders.size() > 0;

    if (!skeyHashVec.empty()) {
        _skeyContext = POOL_COMPATIBLE_NEW_CLASS(_pool, SKeyContext, _pool);
        _skeyContext->Init(skeyHashVec);
        _foundSKeys.reserve(skeyHashVec.size());
    }
    if (curTimeInSecond > ttl && !indexConfig->StoreExpireTime()) {
        _minTsInSecond = curTimeInSecond - ttl;
    }

    if (_metricsCollector != nullptr) {
        _metricsCollector->BeginMemTableQuery();
        _metricsCollector->ResetBlockCounter();
    }
    _isValid = SwitchIterator();
}

template <typename SKeyType>
NormalKKVIteratorImpl<SKeyType>::~NormalKKVIteratorImpl()
{
    POOL_COMPATIBLE_DELETE_CLASS(_pool, _curSegIterator);
    POOL_COMPATIBLE_DELETE_CLASS(_pool, _skeyContext);
}

template <typename SKeyType>
void NormalKKVIteratorImpl<SKeyType>::BatchGet(KKVResultBuffer& resultBuffer)
{
    assert(!resultBuffer.ReachLimit() && !resultBuffer.IsFull());

    while (IsValid()) {
        _curSegIterator->BatchGet(_skeyContext, _minTsInSecond, _curTsInSecond, _foundSKeys, resultBuffer);
        if (resultBuffer.IsFull()) {
            // 待buffer被消费完后, 继续遍历当前segment
            break;
        }
        assert(!_curSegIterator->IsValid());

        if (resultBuffer.ReachLimit()) {
            _isValid = false;
            break;
        }
        if (_curSegIterator->IsLastSegment()) {
            _isValid = false;
            break;
        }
        if (_skeyContext && _skeyContext->GetRequiredSKeyCount() == _foundSKeys.size()) {
            _isValid = false;
            break;
        }

        _isValid = SwitchIterator();
    }
    // TODO(xinfei.sxf) 指标在最后统计，使得memtable count，sstable count指标错误。
    if (_metricsCollector != nullptr) {
        _metricsCollector->IncResultCount(resultBuffer.Size());
    }
}

template <typename SKeyType>
bool NormalKKVIteratorImpl<SKeyType>::SwitchIterator()
{
    if (_searchingBuildingSeg) {
        if (SwitchBuildingSegmentIterator()) {
            return true;
        }
        _searchingBuildingSeg = false;
    }

    return SwitchBuiltSegmentIterator();
}

template <typename SKeyType>
bool NormalKKVIteratorImpl<SKeyType>::SwitchBuildingSegmentIterator()
{
    assert(_searchingBuildingSeg);
    POOL_COMPATIBLE_DELETE_CLASS(_pool, _curSegIterator);
    _curSegIterator = nullptr;
    while (++_curBuildingSegIdx < (int32_t)_buildingSegReaders.size()) {
        _curSegIterator = _buildingSegReaders[_curBuildingSegIdx].first->Lookup(_pkey, _pool, _metricsCollector);
        if (_curSegIterator == nullptr) {
            continue;
        }

        // 查询性能优化
        if (_curSegIterator->HasPKeyDeleted()) {
            _curSegIterator->SetIsLastSegment(true);
        }
        if (_hasOnlySegment) {
            _curSegIterator->SetIsOnlySegment(true);
        }
        return true;
    }
    return false;
}

template <typename SKeyType>
bool NormalKKVIteratorImpl<SKeyType>::SwitchBuiltSegmentIterator()
{
    assert(!_searchingBuildingSeg);
    if (_metricsCollector != nullptr) {
        _metricsCollector->BeginSSTableQuery();
    }

    POOL_COMPATIBLE_DELETE_CLASS(_pool, _curSegIterator);
    _curSegIterator = nullptr;
    while (--_curBuiltSegIdx >= 0) {
        if (IsObseleteBuiltSegment(_curBuiltSegIdx)) {
            continue;
        }

        auto [status, curSegIterator] = future_lite::interface::syncAwait(
            _builtSegReaders[_curBuiltSegIdx].first->Lookup(_pkey, _pool, _metricsCollector));
        // TODO(xinfei.sxf) return error
        if (!status.IsOK()) {
            break;
        }
        _curSegIterator = curSegIterator;
        if (_curSegIterator == nullptr) {
            continue;
        }

        // 查询性能优化
        if (IsLastBuiltSegment()) {
            _curSegIterator->SetIsLastSegment(true);
        }
        if (_hasOnlySegment) {
            _curSegIterator->SetIsOnlySegment(true);
        }
        return true;
    }

    return false;
}

template <typename SKeyType>
bool NormalKKVIteratorImpl<SKeyType>::IsLastBuiltSegment() const
{
    assert(_curSegIterator);
    // 若当前segment中包含pk delete消息，后续的segment不需要遍历
    if (_curSegIterator->HasPKeyDeleted() || _curBuiltSegIdx <= 0) {
        return true;
    }

    int32_t nextBuiltSegIdx = _curBuiltSegIdx - 1;
    for (int32_t i = nextBuiltSegIdx; i >= 0; --i) {
        if (!IsObseleteBuiltSegment(i)) {
            return false;
        }
    }
    return true;
}

template <typename SKeyType>
bool NormalKKVIteratorImpl<SKeyType>::IsObseleteBuiltSegment(int32_t idx) const
{
    assert(idx >= 0 && idx < (int32_t)_builtSegReaders.size());
    return _builtSegReaders[idx].first->GetTimestampInSecond() < _minTsInSecond;
}

} // namespace indexlibv2::index
