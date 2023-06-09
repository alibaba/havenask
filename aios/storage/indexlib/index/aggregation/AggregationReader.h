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

#include <unordered_set>

#include "autil/result/Result.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/index/aggregation/ISegmentReader.h"
#include "indexlib/index/aggregation/KeyMeta.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::framework {
class Segment;
}

namespace indexlibv2::index {

class AttributeReference;
class PackAttributeFormatter;
class PackValueComparator;

class AggregationReader : public IIndexReader
{
public:
    using SegmentReaders = std::vector<std::shared_ptr<ISegmentReader>>;

private:
    enum IteratorHint { IH_AUTO, IH_SEQ, IH_SORT };

public:
    AggregationReader();
    ~AggregationReader();

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) override;
    Status Init(const std::shared_ptr<config::AggregationIndexConfig>& indexConfig,
                const std::vector<std::shared_ptr<framework::Segment>>& memorySegments,
                const std::vector<std::shared_ptr<framework::Segment>>& diskSegments);
    Status Init(const std::shared_ptr<config::AggregationIndexConfig>& indexConfig, SegmentReaders segmentReaders);

public:
    uint64_t CalculateKeyHash(const std::vector<autil::StringView>& keys) const;

    ValueMeta GetMergedValueMeta(uint64_t key) const;

    // for merge
    std::unique_ptr<IKeyIterator> CreateKeyIterator() const;
    const std::shared_ptr<AggregationReader>& GetDeleteReader() const { return _deleteReader; }

    const std::shared_ptr<config::AggregationIndexConfig>& GetIndexConfig() const { return _indexConfig; }
    const std::shared_ptr<PackValueComparator>& GetDataComparator() const { return _cmp; }
    const PackAttributeFormatter* GetPackAttributeFormatter() const { return _packAttrFormatter.get(); }

private:
    Status InitValueReference();

    template <bool inMemory>
    Status CollectSegmentReaders(const std::shared_ptr<config::AggregationIndexConfig>& indexConfig,
                                 const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                 SegmentReaders& segmentReaders, SegmentReaders& deleteReaders);

    std::vector<std::unique_ptr<IValueIterator>> CreateLeafIters(uint64_t key, autil::mem_pool::PoolBase* pool) const;

private:
    std::shared_ptr<config::AggregationIndexConfig> _indexConfig;
    std::unique_ptr<PackAttributeFormatter> _packAttrFormatter;
    std::shared_ptr<PackValueComparator> _cmp;
    SegmentReaders _segmentReaders;
    std::shared_ptr<AggregationReader> _deleteReader;

private:
    friend class ValueIteratorCreator;
};

} // namespace indexlibv2::index
