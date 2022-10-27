#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_READER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_READER_H

#include <iomanip>
#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_creator.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/dimension_description.h"
#include <autil/ConstString.h>

IE_NAMESPACE_BEGIN(index);

template <typename T>
class SingleValueAttributeReader : public AttributeReader
{
public:
    typedef SingleValueAttributeSegmentReader<T> SegmentReader;
    typedef std::tr1::shared_ptr<SegmentReader> SegmentReaderPtr;
    typedef InMemSingleValueAttributeReader<T> InMemSegmentReader;
    typedef std::tr1::shared_ptr<InMemSegmentReader> InMemSegmentReaderPtr;
    typedef AttributeIteratorTyped<T> AttributeIterator;
    typedef BuildingAttributeReader<T> BuildingAttributeReaderType;
    typedef std::tr1::shared_ptr<BuildingAttributeReaderType> BuildingAttributeReaderPtr;

public:
    class Creator : public AttributeReaderCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return common::TypeInfo<T>::GetFieldType();
        }

        AttributeReader* Create(AttributeMetrics* metrics = NULL) const 
        {
            return new SingleValueAttributeReader<T>(metrics);
        }
    };

public:
    SingleValueAttributeReader(AttributeMetrics* metrics = NULL) 
        : AttributeReader(metrics)
        , mSortType(sp_nosort)
        , mBuildingBaseDocId(INVALID_DOCID)
    {
    }

    virtual ~SingleValueAttributeReader() {}

    DECLARE_ATTRIBUTE_READER_IDENTIFIER(single);    
public:
    bool Open(const config::AttributeConfigPtr& attrConfig,
              const index_base::PartitionDataPtr& partitionData) override;

    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override;
    
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) const override;

    AttrType GetType() const override;

    bool IsMultiValue() const override;

    AttributeIterator* CreateIterator(
            autil::mem_pool::Pool *pool) const override;

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen) override;

    bool GetSortedDocIdRange(
            const index_base::RangeDescription& range,
            const DocIdRange& rangeLimit, 
            DocIdRange& resultRange) const override;

    std::string GetAttributeName() const override;

    AttributeSegmentReaderPtr GetSegmentReader(docid_t docId) const;

public:
    inline bool Read(docid_t docId, T& attrValue,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;

    // for test
    BuildingAttributeReaderPtr GetBuildingAttributeReader() const
    { return mBuildingAttributeReader; }

private:
    template <typename Comp1, typename Comp2>
    bool InternalGetSortedDocIdRange(
            const index_base::RangeDescription& range,
            const DocIdRange& rangeLimit, 
            DocIdRange& resultRange) const;

    template <typename Compare>
    bool Search(T value, DocIdRange rangeLimit, docid_t& docId) const;

    virtual SegmentReaderPtr CreateSegmentReader(
            const config::AttributeConfigPtr& attrConfig,
            AttributeMetrics* attrMetrics = NULL)
    {
        return SegmentReaderPtr(new SegmentReader(attrConfig, attrMetrics));
    }

protected:
    virtual void InitBuildingAttributeReader(
            const index_base::SegmentIteratorPtr& segIter);

    file_system::DirectoryPtr GetAttributeDirectory(
        const index_base::SegmentData& segData,
        const config::AttributeConfigPtr& attrConfig) const override;

protected:
    std::vector<SegmentReaderPtr> mSegmentReaders;
    BuildingAttributeReaderPtr mBuildingAttributeReader;
    
    index_base::SegmentInfos mSegmentInfos;
    std::vector<segmentid_t> mSegmentIds;
    config::AttributeConfigPtr mAttrConfig;
    SortPattern mSortType;
    docid_t mBuildingBaseDocId;

private:
    friend class SingleValueAttributeReaderTest;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeReader);

//////////////////////////////////////////////
// inline functions
template<typename T>
bool SingleValueAttributeReader<T>::Open(
        const config::AttributeConfigPtr& attrConfig,
        const index_base::PartitionDataPtr& partitionData)
{
    mAttrConfig = attrConfig;

    std::string attrName = GetAttributeName();
    IE_LOG(DEBUG, "Start opening attribute(%s).", attrName.c_str());

    mSortType = partitionData->GetPartitionMeta().GetSortPattern(attrName);
    index_base::PartitionSegmentIteratorPtr segIter =
        partitionData->CreateSegmentIterator();
    assert(segIter);
    index_base::SegmentIteratorPtr builtSegIter =
        segIter->CreateIterator(index_base::SIT_BUILT);
    assert(builtSegIter);
        
    while (builtSegIter->IsValid())
    {
        const index_base::SegmentData& segData = builtSegIter->GetSegmentData();
        const index_base::SegmentInfo& segmentInfo = segData.GetSegmentInfo();
        if (segmentInfo.docCount == 0)
        {
            builtSegIter->MoveToNext();
            continue;
        }

        try
        {
            SegmentReaderPtr segReader = CreateSegmentReader(mAttrConfig, mAttributeMetrics);
            file_system::DirectoryPtr attrDirectory = GetAttributeDirectory(segData, mAttrConfig);
            segReader->Open(segData, attrDirectory);
            mSegmentReaders.push_back(segReader);
            mSegmentInfos.push_back(segmentInfo);
            mSegmentIds.push_back(segData.GetSegmentId());
            builtSegIter->MoveToNext();
        }
        catch (const misc::NonExistException& e)
        {
            if (mAttrConfig->GetConfigType() == config::AttributeConfig::ct_index_accompany)
            {
                IE_LOG(WARN, "index accompany attribute [%s] open fail: [%s]",
                       mAttrConfig->GetAttrName().c_str(), e.what());
                return false;
            }
            throw;
        }
        catch (const misc::ExceptionBase& e)
        {
            IE_LOG(ERROR, "Load segment FAILED, segment id: [%d], reason: [%s].",
                   segData.GetSegmentId(), e.what());
            throw;
        }
    }

    mBuildingBaseDocId = segIter->GetBuildingBaseDocId();
    index_base::SegmentIteratorPtr buildingSegIter =
        segIter->CreateIterator(index_base::SIT_BUILDING);
    InitBuildingAttributeReader(buildingSegIter);
    IE_LOG(DEBUG, "Finish opening attribute(%s).", attrName.c_str());
    return true;
}

template<typename T>
file_system::DirectoryPtr SingleValueAttributeReader<T>::GetAttributeDirectory(
        const index_base::SegmentData& segData,
        const config::AttributeConfigPtr& attrConfig) const
{
    return segData.GetAttributeDirectory(attrConfig->GetAttrName(),
            attrConfig->GetConfigType() != config::AttributeConfig::ct_index_accompany);
}

template<typename T>
inline void SingleValueAttributeReader<T>::InitBuildingAttributeReader(
        const index_base::SegmentIteratorPtr& buildingIter)
{
    if (!buildingIter)
    {
        return;
    }
    while (buildingIter->IsValid())
    {
        index_base::InMemorySegmentPtr inMemorySegment = buildingIter->GetInMemSegment();
        if (!inMemorySegment)
        {
            buildingIter->MoveToNext();
            continue;
        }
        
        index::InMemorySegmentReaderPtr segmentReader = inMemorySegment->GetSegmentReader();
        if (!segmentReader)
        {
            buildingIter->MoveToNext();
            continue;
        }
        AttributeSegmentReaderPtr attrSegReader = 
            segmentReader->GetAttributeSegmentReader(GetAttributeName());
        assert(attrSegReader);

        InMemSegmentReaderPtr inMemSegReader =
            DYNAMIC_POINTER_CAST(InMemSegmentReader, attrSegReader);
        if (!mBuildingAttributeReader)
        {
            mBuildingAttributeReader.reset(new BuildingAttributeReaderType);
        }
        mBuildingAttributeReader->AddSegmentReader(buildingIter->GetBaseDocId(), inMemSegReader);
        buildingIter->MoveToNext();
    }
}

template<typename T>
bool SingleValueAttributeReader<T>::Read(docid_t docId, uint8_t* buf, uint32_t bufLen) const
{
    if (bufLen >= sizeof(T))
    {
        T& value = *((T*)(buf));
        return Read(docId, value);
    }
    return false;
}

template<typename T>
inline bool SingleValueAttributeReader<T>::Read(
        docid_t docId, T& attrValue, autil::mem_pool::Pool* pool) const
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentInfos.size(); ++i)
    {
        const index_base::SegmentInfo& segInfo = mSegmentInfos[i];
        if (docId < baseDocId + (docid_t)segInfo.docCount)
        {
            return mSegmentReaders[i]->Read(docId - baseDocId, attrValue, pool);
        }
        baseDocId += segInfo.docCount;
    }

    size_t buildingSegIdx = 0;
    return mBuildingAttributeReader &&
        mBuildingAttributeReader->Read(docId, attrValue, buildingSegIdx, pool);
}

template<typename T>
bool SingleValueAttributeReader<T>::Read(docid_t docId, std::string& attrValue,
        autil::mem_pool::Pool* pool) const
{
    T value;
    if (!Read(docId, value, pool))
    {
        return false;
    }
    attrValue = autil::StringUtil::toString<T>(value);
    return true;
}

template <typename T>
template <typename Compare>
inline bool SingleValueAttributeReader<T>::Search(
        T value, DocIdRange rangeLimit, docid_t& docId) const
{
    docid_t baseDocId = 0;
    size_t segId = 0;
    for (; segId < mSegmentInfos.size(); ++segId)
    {
        if (rangeLimit.first >= rangeLimit.second) 
        {
            break;
        }
        docid_t segDocCount = (docid_t)mSegmentInfos[segId].docCount;
        if (rangeLimit.first >= baseDocId 
            && rangeLimit.first < baseDocId + segDocCount)
        {
            docid_t foundSegDocId;
            DocIdRange segRangeLimit;
            segRangeLimit.first = rangeLimit.first - baseDocId;
            segRangeLimit.second = std::min(rangeLimit.second - baseDocId,
                    segDocCount);
            mSegmentReaders[segId]->template Search< Compare > (
                    value, segRangeLimit, foundSegDocId);
            docId = foundSegDocId + baseDocId;
            if (foundSegDocId < segRangeLimit.second) 
            {
                return true;
            }
            rangeLimit.first = baseDocId + segDocCount;
        }
        baseDocId += segDocCount;
    }

    return false;
}

template <typename T>
template <typename Comp1, typename Comp2>
inline bool SingleValueAttributeReader<T>::InternalGetSortedDocIdRange(
        const index_base::RangeDescription& range,
        const DocIdRange& rangeLimit, 
        DocIdRange& resultRange) const
{
    T from = 0;
    T to = 0;
    if (range.from != index_base::RangeDescription::INFINITE
        || range.to != index_base::RangeDescription::INFINITE) {
        if (range.from == index_base::RangeDescription::INFINITE) {
            if (!autil::StringUtil::fromString(range.to, to)) {
                return false;
            }
        } else if (range.to == index_base::RangeDescription::INFINITE) {
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

    if (range.from == index_base::RangeDescription::INFINITE) {
        resultRange.first = rangeLimit.first;
    } else {
        Search< Comp1 >(from, rangeLimit, resultRange.first);
    }

    if (range.to == index_base::RangeDescription::INFINITE) {
        resultRange.second = rangeLimit.second;
    } else {
        Search< Comp2 >(to, rangeLimit, resultRange.second);
    }
    return true;
}

template <typename T>
inline bool SingleValueAttributeReader<T>::GetSortedDocIdRange(
        const index_base::RangeDescription& range,
        const DocIdRange& rangeLimit, 
        DocIdRange& resultRange) const
{
    switch (mSortType) {
    case sp_asc:
        return InternalGetSortedDocIdRange< std::greater<T>, 
                                            std::greater_equal<T> >(
                range, rangeLimit, resultRange);
    case sp_desc:
        return InternalGetSortedDocIdRange< std::less<T>, 
                                            std::less_equal<T> >(
                range, rangeLimit, resultRange);
    default:
        return false;
    }
    return false;
}

template <>
inline bool SingleValueAttributeReader<autil::uint128_t>::GetSortedDocIdRange(
        const index_base::RangeDescription& range,
        const DocIdRange& rangeLimit, 
        DocIdRange& resultRange) const
{
    assert(false);
    return false;
}

template<typename T>
AttrType SingleValueAttributeReader<T>::GetType() const
{        
    return common::TypeInfo<T>::GetAttrType();
}

template<typename T>
bool SingleValueAttributeReader<T>::IsMultiValue() const
{
    return false;
}

template<typename T>
typename SingleValueAttributeReader<T>::AttributeIterator* SingleValueAttributeReader<T>::CreateIterator(
        autil::mem_pool::Pool *pool) const
{
    AttributeIterator* attrIt = IE_POOL_COMPATIBLE_NEW_CLASS(
            pool, AttributeIterator, mSegmentReaders,
            mBuildingAttributeReader, mSegmentInfos, mBuildingBaseDocId, pool);
    return attrIt;
}

template<typename T>
bool SingleValueAttributeReader<T>::UpdateField(docid_t docId, 
        uint8_t* buf, uint32_t bufLen)
{
    if (!this->mAttrConfig->IsAttributeUpdatable())
    {
        IE_LOG(ERROR, "attribute [%s] is not updatable!",
               this->mAttrConfig->GetAttrName().c_str());
        return false;
    }

    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentInfos.size(); i++)
    {
        const index_base::SegmentInfo& segInfo = mSegmentInfos[i];
        if (docId < baseDocId + (docid_t)segInfo.docCount)
        {
            return mSegmentReaders[i]->UpdateField(docId - baseDocId, buf, bufLen);
        }
        baseDocId += segInfo.docCount;
    }
    
    return mBuildingAttributeReader &&
        mBuildingAttributeReader->UpdateField(docId, buf, bufLen);
}

template <typename T>
AttributeSegmentReaderPtr SingleValueAttributeReader<T>::GetSegmentReader(
    docid_t docId) const
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentInfos.size(); i++)
    {
        const index_base::SegmentInfo& segInfo = mSegmentInfos[i];
        if (docId < baseDocId + (docid_t)segInfo.docCount)
        {
            return mSegmentReaders[i];
        }
        baseDocId += segInfo.docCount;
    }
    return AttributeSegmentReaderPtr();
}

template <typename T>
std::string SingleValueAttributeReader<T>::GetAttributeName() const
{
    return mAttrConfig->GetAttrName();
}

typedef SingleValueAttributeReader<float> FloatAttributeReader;
DEFINE_SHARED_PTR(FloatAttributeReader);

typedef SingleValueAttributeReader<double> DoubleAttributeReader;
DEFINE_SHARED_PTR(DoubleAttributeReader);

typedef SingleValueAttributeReader<int64_t> Int64AttributeReader;
DEFINE_SHARED_PTR(Int64AttributeReader);

typedef SingleValueAttributeReader<uint64_t> UInt64AttributeReader;
DEFINE_SHARED_PTR(UInt64AttributeReader);

typedef SingleValueAttributeReader<uint64_t> Hash64AttributeReader;
DEFINE_SHARED_PTR(Hash64AttributeReader);

typedef SingleValueAttributeReader<autil::uint128_t> UInt128AttributeReader;
DEFINE_SHARED_PTR(UInt128AttributeReader);

typedef SingleValueAttributeReader<autil::uint128_t> Hash128AttributeReader;
DEFINE_SHARED_PTR(Hash128AttributeReader);

typedef SingleValueAttributeReader<int32_t> Int32AttributeReader;
DEFINE_SHARED_PTR(Int32AttributeReader);

typedef SingleValueAttributeReader<uint32_t> UInt32AttributeReader;
DEFINE_SHARED_PTR(UInt32AttributeReader);

typedef SingleValueAttributeReader<int16_t> Int16AttributeReader;
DEFINE_SHARED_PTR(Int16AttributeReader);

typedef SingleValueAttributeReader<uint16_t> UInt16AttributeReader;
DEFINE_SHARED_PTR(UInt16AttributeReader);

typedef SingleValueAttributeReader<int8_t> Int8AttributeReader;
DEFINE_SHARED_PTR(Int8AttributeReader);

typedef SingleValueAttributeReader<uint8_t> UInt8AttributeReader;
DEFINE_SHARED_PTR(UInt8AttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_READER_H
