#ifndef __INDEXLIB_LAZY_LOAD_VAR_NUM_ATTRIBUTE_READER_H
#define __INDEXLIB_LAZY_LOAD_VAR_NUM_ATTRIBUTE_READER_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_creator.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_var_num_attribute_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class LazyLoadVarNumAttributeReader : public AttributeReader
{
public:
    typedef MultiValueAttributeSegmentReader<T> SegmentReader;
    typedef std::tr1::shared_ptr<SegmentReader> SegmentReaderPtr;
    typedef InMemVarNumAttributeReader<T> InMemSegmentReader;
    typedef std::tr1::shared_ptr<InMemSegmentReader> InMemSegmentReaderPtr;

    typedef AttributeIteratorTyped<
        autil::MultiValueType<T>, 
        AttributeReaderTraits<autil::MultiValueType<T> > > AttributeIterator;
    typedef std::tr1::shared_ptr<AttributeIterator> AttributeIteratorPtr;
    
    typedef BuildingAttributeReader<
        autil::MultiValueType<T>,
        AttributeReaderTraits<autil::MultiValueType<T> > > BuildingAttributeReaderType;
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
            return new LazyLoadVarNumAttributeReader<T>(metrics);
        }
    };

public:
    LazyLoadVarNumAttributeReader(AttributeMetrics* metrics = NULL) 
        : AttributeReader(metrics)
        , mCurSegmentIdx(-1)
        , mCurSegBaseDocId(INVALID_DOCID)
        , mBuildingBaseDocId(INVALID_DOCID)
    {}

    ~LazyLoadVarNumAttributeReader() {}
    
    DECLARE_ATTRIBUTE_READER_IDENTIFIER(var_num_lazy);

public:
    bool Open(const config::AttributeConfigPtr& attrConfig,
                             const index_base::PartitionDataPtr& partitionData) override;

    bool IsLazyLoad() const override { return true; }

    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override;
    
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) const override;
    AttrType GetType() const override;
    bool IsMultiValue() const override;
    AttributeIteratorBase* CreateIterator(autil::mem_pool::Pool *pool) const override;

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen) override;    

    bool Read(docid_t docId, autil::MultiValueType<T>& value,
              autil::mem_pool::Pool* pool = NULL) const;

    bool Read(docid_t docId, autil::CountedMultiValueType<T>& value,
              autil::mem_pool::Pool* pool = NULL) const;

    bool GetSortedDocIdRange(const index_base::RangeDescription& range,
                             const DocIdRange& rangeLimit,
                             DocIdRange& resultRange) const override
    {
        return false;
    }

    std::string GetAttributeName() const override
    {
        return mAttrConfig->GetAttrName();
    }
    
protected:
    virtual void InitBuildingAttributeReader(
            const index_base::SegmentIteratorPtr& segIter);

private:
    SegmentReaderPtr CreateSegmentReader(
        const index_base::PartitionDataPtr& partitionData,
        segmentid_t segmentId,
        const config::AttributeConfigPtr& attrConfig,
        AttributeMetrics* attrMetrics = NULL) const; 
    
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
    
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, LazyLoadVarNumAttributeReader);

///////////////////////////////////////////////////

template <typename T>
inline typename LazyLoadVarNumAttributeReader<T>::SegmentReaderPtr
LazyLoadVarNumAttributeReader<T>::CreateSegmentReader(
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
        segData.GetAttributeDirectory(attrConfig->GetAttrName(), true);

    AttributeSegmentPatchIteratorPtr patchIterator;
    if (attrConfig->IsAttributeUpdatable())
    {
        patchIterator.reset(new VarNumAttributePatchReader<T>(attrConfig));
        patchIterator->Init(partitionData, segmentId);
    }
    segmentReader->Open(segData, attrDirectory, patchIterator);
    return segmentReader;
}

template<typename T>
bool LazyLoadVarNumAttributeReader<T>::Open(
        const config::AttributeConfigPtr& attrConfig,
        const index_base::PartitionDataPtr& partitionData)
{
    //dup with single value attribute reader
    mAttrConfig = attrConfig;
    mPartitionData = partitionData;
    mCurSegmentIdx = -1;
    mCurSegBaseDocId = INVALID_DOCID;    

    std::string attrName = GetAttributeName();
    IE_LOG(DEBUG, "Start opening attribute(%s).", attrName.c_str());

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
        mSegmentInfos.push_back(segmentInfo);
        mSegmentIds.push_back(segData.GetSegmentId());
        builtSegIter->MoveToNext();
    }

    mBuildingBaseDocId = segIter->GetBuildingBaseDocId();
    index_base::SegmentIteratorPtr buildingSegIter =
        segIter->CreateIterator(index_base::SIT_BUILDING);
    InitBuildingAttributeReader(buildingSegIter);
    IE_LOG(DEBUG, "Finish opening attribute(%s).", attrName.c_str());
    return true;
}

template<typename T>
AttrType LazyLoadVarNumAttributeReader<T>::GetType() const
{        
    return common::TypeInfo<T>::GetAttrType();
}

template<typename T>
bool LazyLoadVarNumAttributeReader<T>::IsMultiValue() const
{
    return true;
}

template<typename T>
bool LazyLoadVarNumAttributeReader<T>::Read(docid_t docId, std::string& attrValue,
        autil::mem_pool::Pool* pool) const
{
    autil::CountedMultiValueType<T> value;
    bool ret = Read(docId, value, pool);
    if (ret)
    {
        uint32_t size = value.size();
        attrValue.clear();
        for (uint32_t i= 0; i < size; i++)
        {
            std::string item = autil::StringUtil::toString<T>(value[i]);
            if (i != 0)
            {
                attrValue += MULTI_VALUE_SEPARATOR;
            }
            attrValue += item;
        }
    }

    return ret;
}

template<typename T>
bool LazyLoadVarNumAttributeReader<T>::Read(
        docid_t docId, uint8_t* buf, uint32_t bufLen) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "LazyLoad VarNumAttributeReader does"
                         " not support Read to buffer");        
    return false;
}

template<typename T>
bool LazyLoadVarNumAttributeReader<T>::Read(
        docid_t docId, autil::MultiValueType<T>& value,
        autil::mem_pool::Pool* pool) const
{
    if (docId >= mBuildingBaseDocId)
    {
        size_t buildingSegIdx = 0;
        return mBuildingAttributeReader &&
            mBuildingAttributeReader->Read(docId, value, buildingSegIdx, pool);
    }
    
    if (mCurSegmentReader &&
        (docId >= mCurSegBaseDocId) &&
        (docId < mCurSegBaseDocId + (docid_t)mSegmentInfos[mCurSegmentIdx].docCount))
    {
        return mCurSegmentReader->Read(docId - mCurSegBaseDocId, value, pool);
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
            return mCurSegmentReader->Read(docId - baseDocId, value, pool);
        }
        baseDocId += segInfo.docCount;
    }
    return false;
}

template<typename T>
bool LazyLoadVarNumAttributeReader<T>::Read(
        docid_t docId, autil::CountedMultiValueType<T>& value,
        autil::mem_pool::Pool* pool) const
{
    if (docId >= mBuildingBaseDocId)
    {
        size_t buildingSegIdx = 0;
        return mBuildingAttributeReader &&
            mBuildingAttributeReader->Read(docId, value, buildingSegIdx, pool);
    }
    
    if (mCurSegmentReader &&
        (docId >= mCurSegBaseDocId) &&
        (docId < mCurSegBaseDocId + (docid_t)mSegmentInfos[mCurSegmentIdx].docCount))
    {
        return mCurSegmentReader->Read(docId - mCurSegBaseDocId, value, pool);
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
            return mCurSegmentReader->Read(docId - baseDocId, value, pool);
        }
        baseDocId += segInfo.docCount;
    }
    return false;
}

template<typename T>
AttributeIteratorBase* LazyLoadVarNumAttributeReader<T>::CreateIterator(
        autil::mem_pool::Pool *pool) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "LazyLoad AttributeReader does"
                         " not support CreateIterator");
    return NULL;
}

template<typename T>
bool LazyLoadVarNumAttributeReader<T>::UpdateField(docid_t docId, 
        uint8_t* buf, uint32_t bufLen)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "LazyLoad AttributeReader does"
                         " not support UpdateField");    
    return false;    
}

template<typename T>
inline void LazyLoadVarNumAttributeReader<T>::InitBuildingAttributeReader(
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

////////////////////////////////////////////////////////////
typedef LazyLoadVarNumAttributeReader<int8_t> LazyLoadInt8MultiValueAttributeReader;
DEFINE_SHARED_PTR(LazyLoadInt8MultiValueAttributeReader);
typedef LazyLoadVarNumAttributeReader<uint8_t> LazyLoadUInt8MultiValueAttributeReader;
DEFINE_SHARED_PTR(LazyLoadUInt8MultiValueAttributeReader);
typedef LazyLoadVarNumAttributeReader<int16_t> LazyLoadInt16MultiValueAttributeReader;
DEFINE_SHARED_PTR(LazyLoadInt16MultiValueAttributeReader);
typedef LazyLoadVarNumAttributeReader<uint16_t> LazyLoadUInt16MultiValueAttributeReader;
DEFINE_SHARED_PTR(LazyLoadUInt16MultiValueAttributeReader);
typedef LazyLoadVarNumAttributeReader<int32_t> LazyLoadInt32MultiValueAttributeReader;
DEFINE_SHARED_PTR(LazyLoadInt32MultiValueAttributeReader);
typedef LazyLoadVarNumAttributeReader<uint32_t> LazyLoadUInt32MultiValueAttributeReader;
DEFINE_SHARED_PTR(LazyLoadUInt32MultiValueAttributeReader);
typedef LazyLoadVarNumAttributeReader<int64_t> LazyLoadInt64MultiValueAttributeReader;
DEFINE_SHARED_PTR(LazyLoadInt64MultiValueAttributeReader);
typedef LazyLoadVarNumAttributeReader<uint64_t> LazyLoadUInt64MultiValueAttributeReader;
DEFINE_SHARED_PTR(LazyLoadUInt64MultiValueAttributeReader);
typedef LazyLoadVarNumAttributeReader<float> LazyLoadFloatMultiValueAttributeReader;
DEFINE_SHARED_PTR(LazyLoadFloatMultiValueAttributeReader);
typedef LazyLoadVarNumAttributeReader<double> LazyLoadDoubleMultiValueAttributeReader;
DEFINE_SHARED_PTR(LazyLoadDoubleMultiValueAttributeReader);
typedef LazyLoadVarNumAttributeReader<autil::MultiChar> LazyLoadMultiStringAttributeReader;
DEFINE_SHARED_PTR(LazyLoadMultiStringAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LAZY_LOAD_VAR_NUM_ATTRIBUTE_READER_H
