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
#include <vector>

#include "autil/Lock.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/index/aggregation/FixedSizeValueBlock.h"
#include "indexlib/index/aggregation/IValueList.h"

namespace indexlibv2::index {
class PackValueComparator;

class FixedSizeValueList final : public IValueList
{
public:
    FixedSizeValueList(autil::mem_pool::PoolBase* pool,
                       const std::shared_ptr<config::AggregationIndexConfig>& indexConfig);
    ~FixedSizeValueList();

public:
    Status Init() override;
    size_t GetTotalCount() const override;
    size_t GetTotalBytes() const override;
    Status Append(const autil::StringView& data) override;
    std::unique_ptr<IValueIterator> CreateIterator(autil::mem_pool::PoolBase* pool) const override;
    StatusOr<ValueStat> Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& file,
                             autil::mem_pool::PoolBase* dumpPool) override;

private:
    int32_t GetValueSize() const;
    uint32_t GetBlockSize() const;
    std::shared_ptr<FixedSizeValueBlock> SealBlock(const std::shared_ptr<FixedSizeValueBlock>& block,
                                                   autil::mem_pool::PoolBase* pool) const;
    std::unique_ptr<IValueIterator> CreateIterator(bool sort, autil::mem_pool::PoolBase* pool) const;

private:
    autil::mem_pool::PoolBase* _pool = nullptr;
    std::shared_ptr<config::AggregationIndexConfig> _indexConfig;
    std::shared_ptr<PackValueComparator> _cmp;
    std::shared_ptr<FixedSizeValueBlock> _block;
    std::vector<std::shared_ptr<FixedSizeValueBlock>> _sealedBlocks;
    size_t _valueCount = 0;
    mutable autil::ReadWriteLock _lock;
};

} // namespace indexlibv2::index
