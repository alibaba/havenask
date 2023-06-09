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
#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_READER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_READER_H

#include <iomanip>
#include <memory>
#include <unordered_map>

#include "autil/ConstString.h"
#include "indexlib/common/field_format/attribute/attribute_field_printer.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_creator.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/temperature_doc_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/normal_table/DimensionDescription.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueAttributeReader : public AttributeReader
{
public:
    typedef SingleValueAttributeSegmentReader<T> SegmentReader;
    typedef std::shared_ptr<SegmentReader> SegmentReaderPtr;
    typedef typename SingleValueAttributeSegmentReader<T>::ReadContext ReadContext;

    typedef InMemSingleValueAttributeReader<T> InMemSegmentReader;
    typedef std::shared_ptr<InMemSegmentReader> InMemSegmentReaderPtr;
    typedef AttributeIteratorTyped<T> AttributeIterator;
    typedef BuildingAttributeReader<T> BuildingAttributeReaderType;
    typedef std::shared_ptr<BuildingAttributeReaderType> BuildingAttributeReaderPtr;

public:
    class Creator : public AttributeReaderCreator
    {
    public:
        FieldType GetAttributeType() const { return common::TypeInfo<T>::GetFieldType(); }

        AttributeReader* Create(AttributeMetrics* metrics = NULL) const
        {
            return new SingleValueAttributeReader<T>(metrics);
        }
    };

public:
    SingleValueAttributeReader(AttributeMetrics* metrics = NULL)
        : AttributeReader(metrics)
        , mSortType(indexlibv2::config::sp_nosort)
        , mBuildingBaseDocId(INVALID_DOCID)
        , mUpdatble(false)
    {
    }

    virtual ~SingleValueAttributeReader() {}

    DECLARE_ATTRIBUTE_READER_IDENTIFIER(single);

public:
    bool Open(const config::AttributeConfigPtr& attrConfig, const index_base::PartitionDataPtr& partitionData,
              PatchApplyStrategy patchApplyStrategy, const AttributeReader* hintReader = nullptr) override;

    bool Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool = NULL) const override;

    AttrType GetType() const override;

    bool IsMultiValue() const override;

    AttributeIterator* CreateIterator(autil::mem_pool::Pool* pool) const override;

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull = false) override;

    bool Updatable() const override { return mUpdatble; }

    bool GetSortedDocIdRange(const indexlib::index::RangeDescription& range, const DocIdRange& rangeLimit,
                             DocIdRange& resultRange) const override;

    std::string GetAttributeName() const override;

    AttributeSegmentReaderPtr GetSegmentReader(docid_t docId) const override;

    void EnableAccessCountors() override;

    void EnableGlobalReadContext() override;

public:
    inline bool Read(docid_t docId, T& attrValue, bool& isNull,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;

    // for test
    BuildingAttributeReaderPtr GetBuildingAttributeReader() const { return mBuildingAttributeReader; }

private:
    template <typename Comp1, typename Comp2>
    bool InternalGetSortedDocIdRange(const indexlib::index::RangeDescription& range, const DocIdRange& rangeLimit,
                                     DocIdRange& resultRange) const;

    template <typename Compare>
    bool Search(T value, DocIdRange rangeLimit, docid_t& docId) const;

    docid_t FindNotNullValueDocId(const DocIdRange& rangeLimit) const;

    virtual SegmentReaderPtr CreateSegmentReader(const config::AttributeConfigPtr& attrConfig,
                                                 AttributeMetrics* attrMetrics = NULL,
                                                 TemperatureProperty property = UNKNOWN)
    {
        return SegmentReaderPtr(new SegmentReader(attrConfig, attrMetrics, property));
    }

protected:
    virtual void InitBuildingAttributeReader(const index_base::SegmentIteratorPtr& segIter);

    file_system::DirectoryPtr GetAttributeDirectory(const index_base::SegmentData& segData,
                                                    const config::AttributeConfigPtr& attrConfig) const override;

protected:
    std::vector<SegmentReaderPtr> mSegmentReaders;
    BuildingAttributeReaderPtr mBuildingAttributeReader;
    common::AttributeFieldPrinter mFieldPrinter;

    std::vector<uint64_t> mSegmentDocCount;
    std::vector<segmentid_t> mSegmentIds;
    config::AttributeConfigPtr mAttrConfig;
    indexlibv2::config::SortPattern mSortType;
    docid_t mBuildingBaseDocId;
    std::string mNullFieldLiteralString;
    bool mUpdatble;

private:
    friend class SingleValueAttributeReaderTest;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeReader);

//////////////////////////////////////////////
// inline functions
template <typename T>
bool SingleValueAttributeReader<T>::Open(const config::AttributeConfigPtr& attrConfig,
                                         const index_base::PartitionDataPtr& partitionData,
                                         PatchApplyStrategy patchApplyStrategy, const AttributeReader* hintReader)
{
    mAttrConfig = attrConfig;
    mUpdatble = true;
    mFieldPrinter.Init(attrConfig->GetFieldConfig());
    mTemperatureDocInfo = partitionData->GetTemperatureDocInfo();

    std::string attrName = GetAttributeName();
    IE_LOG(DEBUG, "Start opening attribute(%s).", attrName.c_str());

    mSortType = partitionData->GetPartitionMeta().GetSortPattern(attrName);
    index_base::PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    index_base::SegmentIteratorPtr builtSegIter = segIter->CreateIterator(index_base::SIT_BUILT);
    assert(builtSegIter);
    std::unordered_map<segmentid_t, SegmentReaderPtr> hintReaderMap;
    if (hintReader) {
        auto typedHintReader = dynamic_cast<const SingleValueAttributeReader*>(hintReader);
        for (size_t i = 0; i < typedHintReader->mSegmentIds.size(); ++i) {
            hintReaderMap[typedHintReader->mSegmentIds[i]] = typedHintReader->mSegmentReaders[i];
        }
    }
    while (builtSegIter->IsValid()) {
        const index_base::SegmentData& segData = builtSegIter->GetSegmentData();
        const std::shared_ptr<const index_base::SegmentInfo>& segmentInfo = segData.GetSegmentInfo();
        if (segmentInfo->docCount == 0) {
            builtSegIter->MoveToNext();
            continue;
        }

        try {
            SegmentReaderPtr segReader;
            auto iter = hintReaderMap.find(segData.GetSegmentId());
            if (iter != hintReaderMap.end()) {
                segReader = iter->second;
            } else {
                TemperatureProperty property = UNKNOWN;
                if (mAttributeMetrics && mTemperatureDocInfo && !mTemperatureDocInfo->IsEmptyInfo()) {
                    property = mTemperatureDocInfo->GetTemperaturePropery(segData.GetSegmentId());
                }
                segReader = CreateSegmentReader(mAttrConfig, mAttributeMetrics, property);
                file_system::DirectoryPtr attrDirectory = GetAttributeDirectory(segData, mAttrConfig);
                PatchApplyOption patchOption;
                patchOption.applyStrategy = patchApplyStrategy;
                if (patchOption.applyStrategy == PatchApplyStrategy::PAS_APPLY_ON_READ) {
                    AttributeSegmentPatchIteratorPtr patchIterator(new SingleValueAttributePatchReader<T>(mAttrConfig));
                    patchIterator->Init(partitionData, segData.GetSegmentId());
                    patchOption.patchReader = patchIterator;
                }
                segReader->Open(segData, patchOption, attrDirectory, /*disableUpdate*/ false);
            }
            if (!segReader->Updatable()) {
                mUpdatble = false;
            }
            mSegmentReaders.push_back(segReader);
            mSegmentDocCount.push_back((uint64_t)segmentInfo->docCount);
            mSegmentIds.push_back(segData.GetSegmentId());
            builtSegIter->MoveToNext();
        } catch (const util::NonExistException& e) {
            if (mAttrConfig->GetConfigType() == config::AttributeConfig::ct_index_accompany) {
                IE_LOG(WARN, "index accompany attribute [%s] open fail: [%s]", mAttrConfig->GetAttrName().c_str(),
                       e.what());
                return false;
            }
            throw;
        } catch (const util::ExceptionBase& e) {
            IE_LOG(ERROR, "Load segment FAILED, segment id: [%d], attribute: [%s], reason: [%s].",
                   segData.GetSegmentId(), GetAttributeName().c_str(), e.what());
            throw;
        }
    }

    mBuildingBaseDocId = segIter->GetBuildingBaseDocId();
    index_base::SegmentIteratorPtr buildingSegIter = segIter->CreateIterator(index_base::SIT_BUILDING);
    InitBuildingAttributeReader(buildingSegIter);
    mNullFieldLiteralString = mAttrConfig->GetFieldConfig()->GetNullFieldLiteralString();
    IE_LOG(DEBUG, "Finish opening attribute(%s).", attrName.c_str());
    return true;
}

template <typename T>
file_system::DirectoryPtr
SingleValueAttributeReader<T>::GetAttributeDirectory(const index_base::SegmentData& segData,
                                                     const config::AttributeConfigPtr& attrConfig) const
{
    return segData.GetAttributeDirectory(attrConfig->GetAttrName(),
                                         attrConfig->GetConfigType() != config::AttributeConfig::ct_index_accompany);
}

template <typename T>
inline void
SingleValueAttributeReader<T>::InitBuildingAttributeReader(const index_base::SegmentIteratorPtr& buildingIter)
{
    if (!buildingIter) {
        return;
    }
    while (buildingIter->IsValid()) {
        index_base::InMemorySegmentPtr inMemorySegment = buildingIter->GetInMemSegment();
        if (!inMemorySegment) {
            buildingIter->MoveToNext();
            continue;
        }

        index_base::InMemorySegmentReaderPtr segmentReader = inMemorySegment->GetSegmentReader();
        if (!segmentReader) {
            buildingIter->MoveToNext();
            continue;
        }
        AttributeSegmentReaderPtr attrSegReader = segmentReader->GetAttributeSegmentReader(GetAttributeName());
        assert(attrSegReader);

        InMemSegmentReaderPtr inMemSegReader = DYNAMIC_POINTER_CAST(InMemSegmentReader, attrSegReader);
        if (!mBuildingAttributeReader) {
            mBuildingAttributeReader.reset(new BuildingAttributeReaderType);
        }
        mBuildingAttributeReader->AddSegmentReader(buildingIter->GetBaseDocId(), inMemSegReader);
        buildingIter->MoveToNext();
    }
}

template <typename T>
inline bool SingleValueAttributeReader<T>::Read(docid_t docId, T& attrValue, bool& isNull,
                                                autil::mem_pool::Pool* pool) const
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentDocCount.size(); ++i) {
        uint64_t docCount = mSegmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            ReadContext* globalCtx = (ReadContext*)mSegmentReaders[i]->GetGlobalReadContext();
            if (globalCtx) {
                return mSegmentReaders[i]->Read(docId - baseDocId, attrValue, isNull, *globalCtx);
            }
            auto ctx = mSegmentReaders[i]->CreateReadContext(pool);
            return mSegmentReaders[i]->Read(docId - baseDocId, attrValue, isNull, ctx);
        }
        baseDocId += docCount;
    }
    size_t buildingSegIdx = 0;
    return mBuildingAttributeReader && mBuildingAttributeReader->Read(docId, attrValue, buildingSegIdx, isNull, pool);
}

template <typename T>
bool SingleValueAttributeReader<T>::Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const
{
    bool isNull = false;
    T value;
    if (!Read(docId, value, isNull, pool)) {
        return false;
    }
    return mFieldPrinter.Print(isNull, value, &attrValue);
}

template <typename T>
template <typename Compare>
inline bool SingleValueAttributeReader<T>::Search(T value, DocIdRange rangeLimit, docid_t& docId) const
{
    docid_t baseDocId = 0;
    size_t segId = 0;
    for (; segId < mSegmentDocCount.size(); ++segId) {
        if (rangeLimit.first >= rangeLimit.second) {
            break;
        }
        docid_t segDocCount = (docid_t)mSegmentDocCount[segId];
        if (rangeLimit.first >= baseDocId && rangeLimit.first < baseDocId + segDocCount) {
            docid_t foundSegDocId;
            DocIdRange segRangeLimit;
            segRangeLimit.first = rangeLimit.first - baseDocId;
            segRangeLimit.second = std::min(rangeLimit.second - baseDocId, segDocCount);
            mSegmentReaders[segId]->template Search<Compare>(value, segRangeLimit, mSortType, foundSegDocId);
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
inline docid_t SingleValueAttributeReader<T>::FindNotNullValueDocId(const DocIdRange& rangeLimit) const
{
    docid_t baseDocId = 0;
    size_t segId = 0;
    for (; segId < mSegmentDocCount.size(); ++segId) {
        docid_t segDocCount = (docid_t)mSegmentDocCount[segId];
        if (rangeLimit.first >= baseDocId && rangeLimit.first < baseDocId + segDocCount) {
            int32_t nullCount = mSegmentReaders[segId]->SearchNullCount(mSortType);
            if (mSortType == indexlibv2::config::sp_asc) {
                return baseDocId + nullCount;
            } else {
                return baseDocId + segDocCount - nullCount;
            }
        }
        baseDocId += segDocCount;
    }
    if (indexlibv2::config::sp_asc) {
        return rangeLimit.first;
    }
    return rangeLimit.second;
}

template <typename T>
template <typename Comp1, typename Comp2>
inline bool SingleValueAttributeReader<T>::InternalGetSortedDocIdRange(const indexlib::index::RangeDescription& range,
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
        if (mAttrConfig->SupportNull() && mSortType == indexlibv2::config::sp_asc) {
            resultRange.first = FindNotNullValueDocId(rangeLimit);
        } else {
            resultRange.first = rangeLimit.first;
        }
    } else {
        Search<Comp1>(from, rangeLimit, resultRange.first);
    }

    if (range.to == indexlib::index::RangeDescription::INFINITE) {
        if (mAttrConfig->SupportNull() && mSortType == indexlibv2::config::sp_desc) {
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
inline bool SingleValueAttributeReader<T>::GetSortedDocIdRange(const indexlib::index::RangeDescription& range,
                                                               const DocIdRange& rangeLimit,
                                                               DocIdRange& resultRange) const
{
    switch (mSortType) {
    case indexlibv2::config::sp_asc:
        return InternalGetSortedDocIdRange<std::greater<T>, std::greater_equal<T>>(range, rangeLimit, resultRange);
    case indexlibv2::config::sp_desc:
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
AttrType SingleValueAttributeReader<T>::GetType() const
{
    return common::TypeInfo<T>::GetAttrType();
}

template <typename T>
bool SingleValueAttributeReader<T>::IsMultiValue() const
{
    return false;
}

template <typename T>
void SingleValueAttributeReader<T>::EnableAccessCountors()
{
    mEnableAccessCountors = true;
    for (size_t i = 0; i < mSegmentReaders.size(); i++) {
        mSegmentReaders[i]->EnableAccessCountors();
    }
}

template <typename T>
void SingleValueAttributeReader<T>::EnableGlobalReadContext()
{
    for (size_t i = 0; i < mSegmentReaders.size(); i++) {
        mSegmentReaders[i]->EnableGlobalReadContext();
    }
}

template <typename T>
typename SingleValueAttributeReader<T>::AttributeIterator*
SingleValueAttributeReader<T>::CreateIterator(autil::mem_pool::Pool* pool) const
{
    AttributeIterator* attrIt =
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, AttributeIterator, mSegmentReaders, mBuildingAttributeReader,
                                     mSegmentDocCount, mBuildingBaseDocId, &mFieldPrinter, pool);
    return attrIt;
}

template <typename T>
bool SingleValueAttributeReader<T>::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull)
{
    if (!mUpdatble) {
        return false;
    }
    if (!mAttrConfig->IsAttributeUpdatable()) {
        IE_LOG(ERROR, "attribute [%s] is not updatable!", mAttrConfig->GetAttrName().c_str());
        return false;
    }

    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentDocCount.size(); i++) {
        uint64_t docCount = mSegmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            return mSegmentReaders[i]->UpdateField(docId - baseDocId, buf, bufLen, isNull);
        }
        baseDocId += docCount;
    }
    return mBuildingAttributeReader && mBuildingAttributeReader->UpdateField(docId, buf, bufLen, isNull);
}

template <typename T>
AttributeSegmentReaderPtr SingleValueAttributeReader<T>::GetSegmentReader(docid_t docId) const
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentDocCount.size(); i++) {
        uint64_t docCount = mSegmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            return mSegmentReaders[i];
        }
        baseDocId += docCount;
    }
    return AttributeSegmentReaderPtr();
}

template <typename T>
std::string SingleValueAttributeReader<T>::GetAttributeName() const
{
    return mAttrConfig->GetAttrName();
}

}} // namespace indexlib::index

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_READER_H
