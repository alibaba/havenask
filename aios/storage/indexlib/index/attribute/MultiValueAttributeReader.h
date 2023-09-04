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

#include "autil/Log.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/attribute/AttributeReaderCreator.h"
#include "indexlib/index/attribute/MultiValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/MultiValueAttributeMemIndexer.h"
#include "indexlib/index/attribute/MultiValueAttributeMemReader.h"
#include "indexlib/index/attribute/SequenceIterator.h"
#include "indexlib/index/attribute/UniqEncodeSequenceIteratorTyped.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"

namespace indexlibv2::index {

template <typename T>
class MultiValueAttributeReader : public AttributeReader
{
public:
    MultiValueAttributeReader(config::SortPattern sortType) : AttributeReader(sortType) {}
    ~MultiValueAttributeReader() = default;

    using SegmentReader = MultiValueAttributeDiskIndexer<T>;
    using ReadContext = typename MultiValueAttributeDiskIndexer<T>::ReadContext;

    using InMemSegmentReader = MultiValueAttributeMemReader<T>;
    using AttributeIterator =
        AttributeIteratorTyped<autil::MultiValueType<T>, AttributeReaderTraits<autil::MultiValueType<T>>>;

public:
    class Creator : public AttributeReaderCreator
    {
    public:
        FieldType GetAttributeType() const override { return TypeInfo<T>::GetFieldType(); }

        std::unique_ptr<AttributeReader> Create(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                const IndexerParameter& indexerParam) const override
        {
            return std::make_unique<MultiValueAttributeReader<T>>(GetSortPattern(indexConfig, indexerParam));
        }
    };

public:
    AttrType GetType() const override;
    bool IsMultiValue() const override;
    AttributeIterator* CreateIterator(autil::mem_pool::Pool* pool) const override;
    std::unique_ptr<AttributeIteratorBase> CreateSequentialIterator() const override;
    bool Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const override;
    bool Read(docid_t docId, autil::MultiValueType<T>& value, autil::mem_pool::Pool* pool) const;

    bool GetSortedDocIdRange(const indexlib::index::RangeDescription& range, const DocIdRange& rangeLimit,
                             DocIdRange& resultRange) const override
    {
        return false;
    }

    std::string GetAttributeName() const override { return _attrConfig->GetAttrName(); }

    std::shared_ptr<AttributeDiskIndexer> TEST_GetIndexer(docid_t docId) const override;

    bool IsIntegrated() const { return _isIntegrated; }

    void EnableAccessCountors() override;

    void EnableGlobalReadContext() override;

    DECLARE_ATTRIBUTE_READER_IDENTIFIER(multi);

protected:
    Status DoOpen(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                  const std::vector<IndexerInfo>& indexers) override;

protected:
    std::vector<std::shared_ptr<MultiValueAttributeDiskIndexer<T>>> _onDiskIndexers;
    std::vector<std::pair<docid_t, std::shared_ptr<MultiValueAttributeMemReader<T>>>> _memReaders;
    std::shared_ptr<DefaultValueAttributeMemReader> _defaultValueReader;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MultiValueAttributeReader, T);

template <typename T>
Status MultiValueAttributeReader<T>::DoOpen(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                            const std::vector<IndexerInfo>& indexers)
{
    auto attrConfig = std::dynamic_pointer_cast<AttributeConfig>(indexConfig);
    assert(attrConfig != nullptr);
    return InitAllIndexers<MultiValueAttributeDiskIndexer<T>, MultiValueAttributeMemReader<T>>(
        attrConfig, indexers, _onDiskIndexers, _memReaders, _defaultValueReader);
}

template <typename T>
AttrType MultiValueAttributeReader<T>::GetType() const
{
    return TypeInfo<T>::GetAttrType();
}
template <typename T>
inline bool MultiValueAttributeReader<T>::IsMultiValue() const
{
    return true;
}
template <>
inline bool MultiValueAttributeReader<char>::IsMultiValue() const
{
    return false;
}

template <typename T>
bool MultiValueAttributeReader<T>::Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<T> value;
    bool ret = Read(docId, value, pool);
    if (!ret) {
        AUTIL_LOG(ERROR, "read value from attr[%s] docId [%d] fail!", GetAttributeName().c_str(), docId);
        return false;
    }
    return _fieldPrinter.Print(value.isNull(), value, &attrValue);
}
template <typename T>
bool MultiValueAttributeReader<T>::Read(docid_t docId, autil::MultiValueType<T>& value,
                                        autil::mem_pool::Pool* pool) const
{
    docid_t baseDocId = 0;
    bool isNull = false;
    for (size_t i = 0; i < _segmentDocCount.size(); ++i) {
        uint64_t docCount = _segmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            auto globalCtx = dynamic_cast<typename MultiValueAttributeDiskIndexer<T>::ReadContext*>(
                _onDiskIndexers[i]->GetGlobalReadContext());
            if (globalCtx) {
                return _onDiskIndexers[i]->Read(docId - baseDocId, value, isNull, *globalCtx);
            }
            auto ctx = _onDiskIndexers[i]->CreateReadContext(pool);
            return _onDiskIndexers[i]->Read(docId - baseDocId, value, isNull, ctx);
        }
        baseDocId += docCount;
    }

    for (auto& [baseDocId, memReader] : _memReaders) {
        if (docId < baseDocId) {
            break;
        }
        if (memReader->Read(docId - baseDocId, value, isNull, pool)) {
            return true;
        }
    }

    if (_defaultValueReader) {
        return _defaultValueReader->ReadMultiValue<T>(docId - baseDocId, value, isNull);
    }

    return false;
}
template <typename T>
std::shared_ptr<AttributeDiskIndexer> MultiValueAttributeReader<T>::TEST_GetIndexer(docid_t docId) const
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
void MultiValueAttributeReader<T>::EnableAccessCountors()
{
    _enableAccessCountors = true;
    for (auto& indexer : _onDiskIndexers) {
        indexer->EnableAccessCountors();
    }
}
template <typename T>
void MultiValueAttributeReader<T>::EnableGlobalReadContext()
{
    for (auto& indexer : _onDiskIndexers) {
        indexer->EnableGlobalReadContext();
    }
    _enableGlobalContext = true;
}

template <typename T>
typename MultiValueAttributeReader<T>::AttributeIterator*
MultiValueAttributeReader<T>::CreateIterator(autil::mem_pool::Pool* pool) const
{
    AttributeIterator* attrIt =
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, AttributeIterator, _onDiskIndexers, _memReaders, _defaultValueReader,
                                     _segmentDocCount, &_fieldPrinter, pool);
    return attrIt;
}

template <typename T>
std::unique_ptr<AttributeIteratorBase> MultiValueAttributeReader<T>::CreateSequentialIterator() const
{
    if (!_enableGlobalContext) {
        AUTIL_LOG(ERROR, "create sequentail iter need global context");
        return nullptr;
    }
    if (_attrConfig->IsUniqEncode()) {
        return std::make_unique<UniqEncodeSequenceIteratorTyped<T, AttributeReaderTraits<autil::MultiValueType<T>>>>(
            _onDiskIndexers, _segmentDocCount, &_fieldPrinter);
    }
    std::vector<std::shared_ptr<AttributeDiskIndexer>> diskIndexers;
    for (const auto& diskIndexer : _onDiskIndexers) {
        diskIndexers.push_back(diskIndexer);
    }
    return std::make_unique<SequenceIterator>(diskIndexers, _segmentDocCount);
}

} // namespace indexlibv2::index
