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
#include "indexlib/index/aggregation/FixedSizeValueList.h"

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/aggregation/SequentialMultiSegmentValueIterator.h"
#include "indexlib/index/aggregation/SortedMultiSegmentValueIterator.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueComparator.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {

FixedSizeValueList::FixedSizeValueList(autil::mem_pool::PoolBase* pool,
                                       const std::shared_ptr<config::AggregationIndexConfig>& indexConfig)
    : _pool(pool)
    , _indexConfig(indexConfig)
{
}

FixedSizeValueList::~FixedSizeValueList() = default;

Status FixedSizeValueList::Init()
{
    const auto& sortDescriptions = _indexConfig->GetSortDescriptions();
    bool needSort = !sortDescriptions.empty();
    if (needSort) {
        _cmp.reset(new PackValueComparator);
        if (!_cmp->Init(_indexConfig->GetValueConfig(), sortDescriptions)) {
            return {
                Status::InternalError("%s: create pack value comparator failed", _indexConfig->GetIndexName().c_str())};
        }
    }

    int32_t valueSize = GetValueSize();
    if (valueSize < 0) {
        return Status::InternalError("expect fixed-size");
    }
    if (GetBlockSize() <= 0) {
        return Status::ConfigError("block size must be specified");
    }
    _block = std::make_shared<FixedSizeValueBlock>(GetBlockSize(), GetValueSize(), _pool);
    return Status::OK();
}

size_t FixedSizeValueList::GetTotalCount() const { return _valueCount; }

size_t FixedSizeValueList::GetTotalBytes() const { return _valueCount * GetValueSize(); }

Status FixedSizeValueList::Append(const autil::StringView& data)
{
    if (data.size() != GetValueSize()) {
        return Status::InternalError("input invalid, expect value size: %u, actual, %u", GetValueSize(),
                                     (uint32_t)data.size());
    }
    if (_block->IsFull()) {
        auto sealedBlock = SealBlock(_block, _pool);
        autil::ScopedWriteLock lock(_lock);
        _sealedBlocks.emplace_back(sealedBlock);
        _block = std::make_shared<FixedSizeValueBlock>(GetBlockSize(), GetValueSize(), _pool);
    }
    _block->Append(data);
    ++_valueCount;
    return Status::OK();
}

std::unique_ptr<IValueIterator> FixedSizeValueList::CreateIterator(autil::mem_pool::PoolBase* pool) const
{
    return CreateIterator(false, pool);
}

std::unique_ptr<IValueIterator> FixedSizeValueList::CreateIterator(bool sort, autil::mem_pool::PoolBase* pool) const
{
    std::vector<std::shared_ptr<FixedSizeValueBlock>> sealedBlocks;
    std::shared_ptr<FixedSizeValueBlock> buildingBlock;
    {
        autil::ScopedReadLock lock(_lock);
        sealedBlocks = _sealedBlocks;
        buildingBlock = _block;
    }
    if (buildingBlock) {
        sealedBlocks.push_back(SealBlock(buildingBlock, pool));
    }
    std::vector<std::unique_ptr<IValueIterator>> iters;
    for (auto it = sealedBlocks.begin(); it != sealedBlocks.end(); ++it) {
        iters.emplace_back((*it)->CreateIterator());
    }
    if (iters.size() == 1) {
        return std::move(iters[0]);
    }
    if (sort && _cmp) {
        return SortedMultiSegmentValueIterator::Create(std::move(iters), _pool, _cmp);
    } else {
        return SequentialMultiSegmentValueIterator::Create(std::move(iters));
    }
}

StatusOr<ValueStat> FixedSizeValueList::Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& file,
                                             autil::mem_pool::PoolBase* dumpPool)
{
    {
        autil::ScopedWriteLock lock(_lock);
        auto sealedBlock = SealBlock(_block, _pool);
        _sealedBlocks.emplace_back(std::move(sealedBlock));
        _block = nullptr;
    }
    ValueStat stat;
    stat.valueCount = _valueCount;
    stat.valueBytes = _valueCount * GetValueSize();
    if (!_cmp) {
        for (const auto& block : _sealedBlocks) {
            RETURN_STATUS_DIRECTLY_IF_ERROR(block->Dump(file));
        }
    } else {
        auto iter = CreateIterator(true, dumpPool);
        bool needDedup = _indexConfig->NeedDedup();
        if (!needDedup) {
            while (iter->HasNext()) {
                autil::StringView value;
                auto s = iter->Next(nullptr, value);
                if (s.IsOK()) {
                    RETURN_STATUS_DIRECTLY_IF_ERROR(file->Write(value.data(), value.size()).Status());
                } else {
                    return {s.steal_error()};
                }
            }
        } else {
            using Allocator = autil::mem_pool::pool_allocator<autil::StringView>;
            using Vector = std::vector<autil::StringView, Allocator>;
            Vector values(dumpPool);
            while (iter->HasNext()) {
                autil::StringView value;
                auto s = iter->Next(nullptr, value);
                if (s.IsOK()) {
                    values.emplace_back(std::move(value));
                }
            }
            auto it = std::unique(values.begin(), values.end());
            values.erase(it, values.end());
            stat.valueCountAfterUnique = values.size();
            stat.valueBytesAfterUnique = values.size() * GetValueSize();
            for (const auto& value : values) {
                RETURN_STATUS_DIRECTLY_IF_ERROR(file->Write(value.data(), value.size()).Status());
            }
        }
    }
    return {std::move(stat)};
}

int32_t FixedSizeValueList::GetValueSize() const
{
    const auto& valueConfig = _indexConfig->GetValueConfig();
    return valueConfig->GetFixedLength();
}

uint32_t FixedSizeValueList::GetBlockSize() const { return _indexConfig->GetRecordCountPerBlock(); }

std::shared_ptr<FixedSizeValueBlock> FixedSizeValueList::SealBlock(const std::shared_ptr<FixedSizeValueBlock>& block,
                                                                   autil::mem_pool::PoolBase* pool) const
{
    if (!_cmp) {
        return block;
    }
    using Allocator = autil::mem_pool::pool_allocator<autil::StringView>;
    std::vector<autil::StringView, Allocator> values(pool);
    size_t size = block->Size();
    values.reserve(size);
    for (auto i = 0; i < size; ++i) {
        values.emplace_back(block->Get(i));
    }
    std::sort(values.begin(), values.end(), [this](const auto& lhs, const auto& rhs) { return (*_cmp)(lhs, rhs); });
    if (_indexConfig->NeedDedup()) {
        auto it = std::unique(values.begin(), values.end());
        values.erase(it, values.end());
    }
    auto sealedBlock = std::make_shared<FixedSizeValueBlock>(values.size(), GetValueSize(), pool);
    for (const auto& value : values) {
        sealedBlock->Append(value);
    }
    return sealedBlock;
}

} // namespace indexlibv2::index
