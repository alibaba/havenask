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
#include <algorithm>
#include <functional>

#include "autil/Log.h"
#include "indexlib/base/Define.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/attribute/AttributeReaderCreator.h"
#include "indexlib/index/attribute/SequenceIterator.h"
#include "indexlib/index/attribute/SingleValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/SingleValueAttributeMemIndexer.h"
#include "indexlib/index/attribute/SingleValueAttributeMemReader.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeReader : public AttributeReader
{
public:
    explicit SingleValueAttributeReader(config::SortPattern sortType) : AttributeReader(sortType) {}

    ~SingleValueAttributeReader() = default;

public:
    using SegmentReader = SingleValueAttributeDiskIndexer<T>;
    using ReadContext = typename SingleValueAttributeDiskIndexer<T>::ReadContext;

    using InMemSegmentReader = SingleValueAttributeMemReader<T>;
    using AttributeIterator = AttributeIteratorTyped<T>;

public:
    class Creator : public AttributeReaderCreator
    {
    public:
        FieldType GetAttributeType() const override { return TypeInfo<T>::GetFieldType(); }

        std::unique_ptr<AttributeReader> Create(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                const IndexReaderParameter& indexReaderParam) const override
        {
            return std::make_unique<SingleValueAttributeReader<T>>(GetSortPattern(indexConfig, indexReaderParam));
        }
    };

public:
    bool Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const override;
    AttrType GetType() const override;
    bool IsMultiValue() const override;
    AttributeIterator* CreateIterator(autil::mem_pool::Pool* pool) const override;
    std::unique_ptr<AttributeIteratorBase> CreateSequentialIterator() const override;
    // rangeLimit = [begin, end), resultRange = [begin, end)
    bool GetSortedDocIdRange(const indexlib::index::RangeDescription& range, const DocIdRange& rangeLimit,
                             DocIdRange& resultRange) const override;
    std::string GetAttributeName() const override;
    std::shared_ptr<AttributeDiskIndexer> TEST_GetIndexer(docid_t docId) const override;
    void EnableAccessCountors() override;
    void EnableGlobalReadContext() override;
    DECLARE_ATTRIBUTE_READER_IDENTIFIER(single);
    bool TEST_Read(docid_t docId, T& attrValue, bool& isNull, autil::mem_pool::Pool* pool) const;

protected:
    Status DoOpen(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                  const std::vector<IndexerInfo>& indexers) override;
    inline bool Read(docid_t docId, T& attrValue, bool& isNull, autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

private:
    template <typename Comp1, typename Comp2>
    bool InternalGetSortedDocIdRange(const indexlib::index::RangeDescription& range, const DocIdRange& rangeLimit,
                                     DocIdRange& resultRange) const;

    template <typename Compare>
    bool Search(T value, DocIdRange rangeLimit, docid_t& docId) const;
    docid_t FindNotNullValueDocId(const DocIdRange& rangeLimit) const;

protected:
    std::vector<std::shared_ptr<SingleValueAttributeDiskIndexer<T>>> _onDiskIndexers;
    std::vector<std::pair<docid_t, std::shared_ptr<SingleValueAttributeMemReader<T>>>> _memReaders;
    std::shared_ptr<DefaultValueAttributeMemReader> _defaultValueReader;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeReader, T);

template <typename T>
Status SingleValueAttributeReader<T>::DoOpen(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                             const std::vector<IndexerInfo>& indexers)
{
    auto attrConfig = std::dynamic_pointer_cast<AttributeConfig>(indexConfig);
    assert(attrConfig != nullptr);
    return InitAllIndexers<SingleValueAttributeDiskIndexer<T>, SingleValueAttributeMemReader<T>>(
        attrConfig, indexers, _onDiskIndexers, _memReaders, _defaultValueReader);
}

template <typename T>
bool SingleValueAttributeReader<T>::Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const
{
    bool isNull = false;
    T value {};
    if (!Read(docId, value, isNull, pool)) {
        return false;
    }
    return _fieldPrinter.Print(isNull, value, &attrValue);
}

template <typename T>
AttrType SingleValueAttributeReader<T>::GetType() const
{
    return TypeInfo<T>::GetAttrType();
}

template <typename T>
bool SingleValueAttributeReader<T>::IsMultiValue() const
{
    return false;
}

template <typename T>
bool SingleValueAttributeReader<T>::TEST_Read(docid_t docId, T& attrValue, bool& isNull,
                                              autil::mem_pool::Pool* pool) const
{
    return Read(docId, attrValue, isNull, pool);
}

template <typename T>
inline bool SingleValueAttributeReader<T>::GetSortedDocIdRange(const indexlib::index::RangeDescription& range,
                                                               const DocIdRange& rangeLimit,
                                                               DocIdRange& resultRange) const
{
    switch (_sortType) {
    case config::sp_asc:
        return InternalGetSortedDocIdRange<std::greater<T>, std::greater_equal<T>>(range, rangeLimit, resultRange);
    case config::sp_desc:
        return InternalGetSortedDocIdRange<std::less<T>, std::less_equal<T>>(range, rangeLimit, resultRange);
    default:
        return false;
    }
    return false;
}

template <>
inline bool SingleValueAttributeReader<autil::uint128_t>::GetSortedDocIdRange(
    const indexlib::index::RangeDescription& range, const DocIdRange& rangeLimit, DocIdRange& resultRange) const
{
    assert(false);
    return false;
}

template <typename T>
std::string SingleValueAttributeReader<T>::GetAttributeName() const
{
    return _attrConfig->GetAttrName();
}

template <typename T>
std::shared_ptr<AttributeDiskIndexer> SingleValueAttributeReader<T>::TEST_GetIndexer(docid_t docId) const
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < _segmentDocCount.size(); i++) {
        uint64_t docCount = _segmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            return _onDiskIndexers[i];
        }
        baseDocId += docCount;
    }
    return nullptr;
}

template <typename T>
void SingleValueAttributeReader<T>::EnableAccessCountors()
{
    _enableAccessCountors = true;
    for (auto& indexer : _onDiskIndexers) {
        indexer->EnableAccessCountors();
    }
}

template <typename T>
inline void SingleValueAttributeReader<T>::EnableGlobalReadContext()
{
    for (auto& indexer : _onDiskIndexers) {
        indexer->EnableGlobalReadContext();
    }
    _enableGlobalContext = true;
}

template <typename T>
inline bool SingleValueAttributeReader<T>::Read(docid_t docId, T& attrValue, bool& isNull,
                                                autil::mem_pool::Pool* pool) const
{
    //   docid range :      [-----------------------------------------------------------------]
    //   _onDiskIndexers:   [disk | disk | disk |                                             ]
    //  _memReaders:        [                   | mem | mem |              | mem |            ]
    //  _defaultValueReader [                               |   default    |     |   default  ]
    docid_t baseDocId = 0;
    for (size_t i = 0; i < _segmentDocCount.size(); ++i) {
        uint64_t docCount = _segmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            auto globalCtx =
                (typename SingleValueAttributeDiskIndexer<T>::ReadContext*)(_onDiskIndexers[i]->GetGlobalReadContext());
            if (globalCtx) {
                return _onDiskIndexers[i]->Read(docId - baseDocId, attrValue, isNull, *globalCtx);
            }
            auto ctx = _onDiskIndexers[i]->CreateReadContext(pool);
            return _onDiskIndexers[i]->Read(docId - baseDocId, attrValue, isNull, ctx);
        }
        baseDocId += docCount;
    }

    for (auto& [baseDocId, memReader] : _memReaders) {
        if (docId < baseDocId) {
            break;
        }
        if (memReader->Read(docId - baseDocId, attrValue, isNull, pool)) {
            return true;
        }
    }

    if (_defaultValueReader) {
        return _defaultValueReader->ReadSingleValue<T>(docId - baseDocId, attrValue, isNull);
    }
    return false;
}

template <typename T>
template <typename Comp1, typename Comp2>
bool SingleValueAttributeReader<T>::InternalGetSortedDocIdRange(const indexlib::index::RangeDescription& range,
                                                                const DocIdRange& rangeLimit,
                                                                DocIdRange& resultRange) const
{
    T from = 0;
    T to = 0;
    if (range.from != indexlib::index::RangeDescription::INFINITE ||
        range.to != indexlib::index::RangeDescription::INFINITE) {
        if (range.from == indexlib::index::RangeDescription::INFINITE) {
            if (!autil::StringUtil::fromString(range.to, to)) {
                return false;
            }
        } else if (range.to == indexlib::index::RangeDescription::INFINITE) {
            if (!autil::StringUtil::fromString(range.from, from)) {
                return false;
            }
        } else {
            if (!autil::StringUtil::fromString(range.from, from)) {
                return false;
            }
            if (!autil::StringUtil::fromString(range.to, to)) {
                return false;
            }
            if (Comp1()(from, to)) {
                std::swap(from, to);
            }
        }
    }

    if (range.from == indexlib::index::RangeDescription::INFINITE) {
        if (_attrConfig->SupportNull() && _sortType == config::sp_asc) {
            resultRange.first = FindNotNullValueDocId(rangeLimit);
        } else {
            resultRange.first = rangeLimit.first;
        }
    } else {
        Search<Comp1>(from, rangeLimit, resultRange.first);
    }

    if (range.to == indexlib::index::RangeDescription::INFINITE) {
        if (_attrConfig->SupportNull() && _sortType == config::sp_desc) {
            resultRange.second = FindNotNullValueDocId(rangeLimit);
        } else {
            resultRange.second = rangeLimit.second;
        }
    } else {
        Search<Comp2>(to, rangeLimit, resultRange.second);
    }
    return true;
}

template <typename T>
template <typename Compare>
bool SingleValueAttributeReader<T>::Search(T value, DocIdRange rangeLimit, docid_t& docId) const
{
    docid_t baseDocId = 0;
    size_t segId = 0;
    for (; segId < _segmentDocCount.size(); ++segId) {
        if (rangeLimit.first >= rangeLimit.second) {
            break;
        }
        docid_t segDocCount = (docid_t)_segmentDocCount[segId];
        if (rangeLimit.first >= baseDocId && rangeLimit.first < baseDocId + segDocCount) {
            docid_t foundSegDocId = INVALID_DOCID;
            DocIdRange segRangeLimit;
            segRangeLimit.first = rangeLimit.first - baseDocId;
            segRangeLimit.second = std::min(rangeLimit.second - baseDocId, segDocCount);
            auto status =
                _onDiskIndexers[segId]->template Search<Compare>(value, segRangeLimit, _sortType, foundSegDocId);
            if (!status.IsOK()) {
                return false;
            }
            docId = foundSegDocId + baseDocId;
            if (foundSegDocId < segRangeLimit.second) {
                return true;
            }
            rangeLimit.first = baseDocId + segDocCount;
        }
        baseDocId += segDocCount;
    }
    return false;
}

template <typename T>
docid_t SingleValueAttributeReader<T>::FindNotNullValueDocId(const DocIdRange& rangeLimit) const
{
    docid_t baseDocId = 0;
    size_t segId = 0;
    for (; segId < _segmentDocCount.size(); ++segId) {
        docid_t segDocCount = (docid_t)_segmentDocCount[segId];
        if (rangeLimit.first >= baseDocId && rangeLimit.first < baseDocId + segDocCount) {
            int32_t nullCount = _onDiskIndexers[segId]->SearchNullCount(_sortType);
            if (_sortType == config::sp_asc) {
                return baseDocId + nullCount;
            } else {
                return baseDocId + segDocCount - nullCount;
            }
        }
        baseDocId += segDocCount;
    }
    if (config::sp_asc) {
        return rangeLimit.first;
    }
    return rangeLimit.second;
}

template <typename T>
typename SingleValueAttributeReader<T>::AttributeIterator*
SingleValueAttributeReader<T>::CreateIterator(autil::mem_pool::Pool* pool) const
{
    AttributeIterator* attrIt =
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, AttributeIterator, _onDiskIndexers, _memReaders, _defaultValueReader,
                                     _segmentDocCount, &_fieldPrinter, pool);
    return attrIt;
}

template <typename T>
std::unique_ptr<AttributeIteratorBase> SingleValueAttributeReader<T>::CreateSequentialIterator() const
{
    if (!_enableGlobalContext) {
        AUTIL_LOG(ERROR, "create sequentail iter need global context");
        return nullptr;
    }
    std::vector<std::shared_ptr<AttributeDiskIndexer>> diskIndexers;
    for (const auto& diskIndexer : _onDiskIndexers) {
        diskIndexers.push_back(diskIndexer);
    }

    return std::make_unique<SequenceIterator>(diskIndexers, _segmentDocCount);
}

} // namespace indexlibv2::index
