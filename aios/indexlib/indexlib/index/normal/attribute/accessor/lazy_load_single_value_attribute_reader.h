#ifndef __INDEXLIB_LAZY_LOAD_SINGLE_VALUE_ATTRIBUTE_READER_H
#define __INDEXLIB_LAZY_LOAD_SINGLE_VALUE_ATTRIBUTE_READER_H

#include <iomanip>
#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_creator.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_patch_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class LazyLoadSingleValueAttributeReader : public AttributeReader
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
            return new LazyLoadSingleValueAttributeReader<T>(metrics);
        }
    };

public:
    LazyLoadSingleValueAttributeReader(AttributeMetrics* metrics = NULL) 
        : AttributeReader(metrics)
        , mCurSegmentIdx(-1)
        , mCurSegBaseDocId(INVALID_DOCID)
        , mBuildingBaseDocId(INVALID_DOCID)
    {
    }

    virtual ~LazyLoadSingleValueAttributeReader() {}

    DECLARE_ATTRIBUTE_READER_IDENTIFIER(single_lazy);    
public:
    bool Open(const config::AttributeConfigPtr& attrConfig,
                             const index_base::PartitionDataPtr& partitionData) override;
    bool IsLazyLoad() const override { return true; }    

    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override;
    
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) const override;

    AttrType GetType() const override;

    bool IsMultiValue() const override;

    AttributeIteratorBase* CreateIterator(
            autil::mem_pool::Pool *pool) const override;

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen) override;

    bool GetSortedDocIdRange(
            const index_base::RangeDescription& range,
            const DocIdRange& rangeLimit, 
            DocIdRange& resultRange) const override;

    std::string GetAttributeName() const override;

public:
    bool Read(docid_t docId, T& attrValue) const;
    
private:
    SegmentReaderPtr CreateSegmentReader(
        const index_base::PartitionDataPtr& partitionData,
        segmentid_t segmentId,
        const config::AttributeConfigPtr& attrConfig,
        AttributeMetrics* attrMetrics = NULL) const; 

protected:
    virtual void InitBuildingAttributeReader(
            const index_base::SegmentIteratorPtr& segIter);
    
    file_system::DirectoryPtr GetAttributeDirectory(
            const index_base::SegmentData& segData,
            const config::AttributeConfigPtr& attrConfig) const override;

protected:
    index_base::PartitionDataPtr mPartitionData;
    mutable SegmentReaderPtr mCurSegmentReader;
    mutable int32_t mCurSegmentIdx;
    mutable docid_t mCurSegBaseDocId;
    
    BuildingAttributeReaderPtr mBuildingAttributeReader;
    index_base::SegmentInfos mSegmentInfos;
    std::vector<segmentid_t> mSegmentIds;
    config::AttributeConfigPtr mAttrConfig;
    docid_t mBuildingBaseDocId;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, LazyLoadSingleValueAttributeReader);

//////////////////////////////////////////////
// inline functions
template <typename T>
inline typename LazyLoadSingleValueAttributeReader<T>::SegmentReaderPtr
LazyLoadSingleValueAttributeReader<T>::CreateSegmentReader(
    const index_base::PartitionDataPtr& partitionData,
    segmentid_t segmentId,
    const config::AttributeConfigPtr& attrConfig,
    AttributeMetrics* attrMetrics) const
{
    assert(partitionData);
    assert(attrConfig);
    SegmentReaderPtr segmentReader(new SegmentReader(attrConfig, attrMetrics));
    const index_base::SegmentData& segData =
        partitionData->GetSegmentData(segmentId); 
    file_system::DirectoryPtr attrDirectory = 
        GetAttributeDirectory(segData, mAttrConfig); 

    AttributeSegmentPatchIteratorPtr patchIterator(
            new SingleValueAttributePatchReader<T>(attrConfig));
    patchIterator->Init(partitionData, segmentId);
    segmentReader->Open(segData, attrDirectory, patchIterator);
    return segmentReader;
}

template<typename T>
bool LazyLoadSingleValueAttributeReader<T>::Open(
        const config::AttributeConfigPtr& attrConfig,
        const index_base::PartitionDataPtr& partitionData)
{
    mAttrConfig = attrConfig;
    mPartitionData = partitionData;
    mCurSegmentIdx = -1;
    mCurSegBaseDocId = INVALID_DOCID;
    std::string attrName = GetAttributeName();
    IE_LOG(DEBUG, "Start opening attribute(%s).", attrName.c_str());

    index_base::PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    index_base::SegmentIteratorPtr builtSegIter = segIter->CreateIterator(index_base::SIT_BUILT);
    assert(builtSegIter);

    while (builtSegIter->IsValid())
    {
        const index_base::SegmentData& segData = builtSegIter->GetSegmentData();
        const index_base::SegmentInfo& segmentInfo = segData.GetSegmentInfo();
        try
        {
            if (segmentInfo.docCount == 0)
            {
                builtSegIter->MoveToNext();
                continue;
            }
            mSegmentInfos.push_back(segmentInfo);
            mSegmentIds.push_back(segData.GetSegmentId());
            builtSegIter->MoveToNext();
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
file_system::DirectoryPtr LazyLoadSingleValueAttributeReader<T>::GetAttributeDirectory(
        const index_base::SegmentData& segData,
        const config::AttributeConfigPtr& attrConfig) const
{
    return segData.GetAttributeDirectory(attrConfig->GetAttrName(),
            attrConfig->GetConfigType() != config::AttributeConfig::ct_index_accompany);
}

template<typename T>
inline void LazyLoadSingleValueAttributeReader<T>::InitBuildingAttributeReader(
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
bool LazyLoadSingleValueAttributeReader<T>::Read(docid_t docId, uint8_t* buf, uint32_t bufLen) const
{
    if (bufLen >= sizeof(T))
    {
        T& value = *((T*)(buf));
        return Read(docId, value);
    }
    return false;
}

template<typename T>
bool LazyLoadSingleValueAttributeReader<T>::Read(docid_t docId, T& attrValue) const
{
    if (docId >= mBuildingBaseDocId)
    {
        size_t buildingSegIdx = 0;
        return mBuildingAttributeReader &&
            mBuildingAttributeReader->Read(docId, attrValue, buildingSegIdx);
    }
    
    if (mCurSegmentReader &&
        (docId >= mCurSegBaseDocId) &&
        (docId < mCurSegBaseDocId + (docid_t)mSegmentInfos[mCurSegmentIdx].docCount))
    {
        return mCurSegmentReader->Read(docId - mCurSegBaseDocId, attrValue);
    }
    
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentInfos.size(); ++i)
    {
        const index_base::SegmentInfo& segInfo = mSegmentInfos[i];
        if (docId < baseDocId + (docid_t)segInfo.docCount)
        {
            mCurSegmentReader.reset();
            mCurSegmentIdx = -1;
            mCurSegBaseDocId = INVALID_DOCID;
            const file_system::DirectoryPtr& rootDir = mPartitionData->GetRootDirectory();
            const file_system::IndexlibFileSystemPtr& fileSystem = rootDir->GetFileSystem();
            fileSystem->CleanCache();
                
            mCurSegmentReader = CreateSegmentReader(
                mPartitionData, mSegmentIds[i], mAttrConfig, mAttributeMetrics);
            mCurSegmentIdx = i;
            mCurSegBaseDocId = baseDocId;
            return mCurSegmentReader->Read(docId - baseDocId, attrValue);
        }
        baseDocId += segInfo.docCount;
    }
    return false;
}

template<typename T>
bool LazyLoadSingleValueAttributeReader<T>::Read(
        docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const
{
    T value;
    bool ret = Read(docId, value);
    if (ret)
    {
        attrValue = autil::StringUtil::toString<T>(value);
    }
    return ret;
}

template <typename T>
inline bool LazyLoadSingleValueAttributeReader<T>::GetSortedDocIdRange(
        const index_base::RangeDescription& range,
        const DocIdRange& rangeLimit, 
        DocIdRange& resultRange) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "LazyLoad AttributeReader does"
                         " not support GetSortedDocIdRange");
    return false;
}

template<typename T>
AttrType LazyLoadSingleValueAttributeReader<T>::GetType() const
{        
    return common::TypeInfo<T>::GetAttrType();
}

template<typename T>
bool LazyLoadSingleValueAttributeReader<T>::IsMultiValue() const
{
    return false;
}

template<typename T>
AttributeIteratorBase* LazyLoadSingleValueAttributeReader<T>::CreateIterator(
        autil::mem_pool::Pool *pool) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "LazyLoad AttributeReader does"
                         " not support CreateIterator");
    return NULL;
}

template<typename T>
bool LazyLoadSingleValueAttributeReader<T>::UpdateField(docid_t docId, 
        uint8_t* buf, uint32_t bufLen)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "LazyLoad AttributeReader does"
                         " not support UpdateField");    
    return false;
}

template <typename T>
std::string LazyLoadSingleValueAttributeReader<T>::GetAttributeName() const
{
    return mAttrConfig->GetAttrName();
}

typedef LazyLoadSingleValueAttributeReader<float> LazyLoadFloatAttributeReader;
DEFINE_SHARED_PTR(LazyLoadFloatAttributeReader);

typedef LazyLoadSingleValueAttributeReader<double> LazyLoadDoubleAttributeReader;
DEFINE_SHARED_PTR(LazyLoadDoubleAttributeReader);

typedef LazyLoadSingleValueAttributeReader<int64_t> LazyLoadInt64AttributeReader;
DEFINE_SHARED_PTR(LazyLoadInt64AttributeReader);

typedef LazyLoadSingleValueAttributeReader<uint64_t> LazyLoadUInt64AttributeReader;
DEFINE_SHARED_PTR(LazyLoadUInt64AttributeReader);

typedef LazyLoadSingleValueAttributeReader<uint64_t> LazyLoadHash64AttributeReader;
DEFINE_SHARED_PTR(LazyLoadHash64AttributeReader);

typedef LazyLoadSingleValueAttributeReader<autil::uint128_t> LazyLoadUInt128AttributeReader;
DEFINE_SHARED_PTR(LazyLoadUInt128AttributeReader);

typedef LazyLoadSingleValueAttributeReader<autil::uint128_t> LazyLoadHash128AttributeReader;
DEFINE_SHARED_PTR(LazyLoadHash128AttributeReader);

typedef LazyLoadSingleValueAttributeReader<int32_t> LazyLoadInt32AttributeReader;
DEFINE_SHARED_PTR(LazyLoadInt32AttributeReader);

typedef LazyLoadSingleValueAttributeReader<uint32_t> LazyLoadUInt32AttributeReader;
DEFINE_SHARED_PTR(LazyLoadUInt32AttributeReader);

typedef LazyLoadSingleValueAttributeReader<int16_t> LazyLoadInt16AttributeReader;
DEFINE_SHARED_PTR(LazyLoadInt16AttributeReader);

typedef LazyLoadSingleValueAttributeReader<uint16_t> LazyLoadUInt16AttributeReader;
DEFINE_SHARED_PTR(LazyLoadUInt16AttributeReader);

typedef LazyLoadSingleValueAttributeReader<int8_t> LazyLoadInt8AttributeReader;
DEFINE_SHARED_PTR(LazyLoadInt8AttributeReader);

typedef LazyLoadSingleValueAttributeReader<uint8_t> LazyLoadUInt8AttributeReader;
DEFINE_SHARED_PTR(LazyLoadUInt8AttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LAZY_LOAD_SINGLE_VALUE_ATTRIBUTE_READER_H
