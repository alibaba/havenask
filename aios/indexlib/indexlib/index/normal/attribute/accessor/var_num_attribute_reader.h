#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_READER_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_READER_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include <autil/DataBuffer.h>
#include <autil/MultiValueType.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_creator.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_var_num_attribute_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class VarNumAttributeReader : public AttributeReader
{
private:
    using AttributeReader::Read;
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
            return new VarNumAttributeReader<T>(metrics);
        }
    };

public:
    VarNumAttributeReader(AttributeMetrics* metrics = NULL) 
        : AttributeReader(metrics)
        , mBuildingBaseDocId(INVALID_DOCID)
    {}

    ~VarNumAttributeReader() {}
    
    DECLARE_ATTRIBUTE_READER_IDENTIFIER(var_num);

public:
    bool Open(const config::AttributeConfigPtr& attrConfig,
              const index_base::PartitionDataPtr& partitionData) override;

    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override;
    
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) const override;
    AttrType GetType() const override;
    bool IsMultiValue() const override;
    AttributeIterator* CreateIterator(autil::mem_pool::Pool *pool) const override;

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen) override;

    bool Read(docid_t docId, autil::MultiValueType<T>& value,
              autil::mem_pool::Pool* pool = NULL) const;

    bool Read(docid_t docId, autil::CountedMultiValueType<T>& value,
              autil::mem_pool::Pool* pool = NULL) const;

    virtual bool GetSortedDocIdRange(const index_base::RangeDescription& range,
            const DocIdRange& rangeLimit, DocIdRange& resultRange) const override
    {
        return false;
    }

    std::string GetAttributeName() const override
    {
        return mAttrConfig->GetAttrName();
    }

    AttributeSegmentReaderPtr GetSegmentReader(docid_t docId) const;    

public:
    // for test
    const std::vector<SegmentReaderPtr>& GetSegmentReaders() const 
    { return mSegmentReaders; }

protected:
    virtual void InitBuildingAttributeReader(
            const index_base::SegmentIteratorPtr& segIter);
protected:
    std::vector<SegmentReaderPtr> mSegmentReaders;
    BuildingAttributeReaderPtr mBuildingAttributeReader;

    index_base::SegmentInfos mSegmentInfos;
    config::AttributeConfigPtr mAttrConfig;
    docid_t mBuildingBaseDocId;
    
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumAttributeReader);

///////////////////////////////////////////////////
template<typename T>
bool VarNumAttributeReader<T>::Open(
        const config::AttributeConfigPtr& attrConfig,
        const index_base::PartitionDataPtr& partitionData)
{
    //dup with single value attribute reader
    mAttrConfig = attrConfig;

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
        if (segmentInfo.docCount == 0)
        {
            builtSegIter->MoveToNext();
            continue;
        }

        try
        {
            SegmentReaderPtr segReader(new SegmentReader(mAttrConfig, mAttributeMetrics));
            file_system::DirectoryPtr attrDirectory = 
                GetAttributeDirectory(segData, mAttrConfig);
            segReader->Open(segData, attrDirectory);
            mSegmentReaders.push_back(segReader);
            mSegmentInfos.push_back(segmentInfo);
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
    index_base::SegmentIteratorPtr buildingSegIter = segIter->CreateIterator(index_base::SIT_BUILDING);
    InitBuildingAttributeReader(buildingSegIter);
    IE_LOG(DEBUG, "Finish opening attribute(%s).", attrName.c_str());
    return true;
}

template<typename T>
AttrType VarNumAttributeReader<T>::GetType() const
{        
    return common::TypeInfo<T>::GetAttrType();
}

template<typename T>
bool VarNumAttributeReader<T>::IsMultiValue() const
{
    return true;
}

template<typename T>
bool VarNumAttributeReader<T>::Read(docid_t docId, std::string& attrValue,
                                    autil::mem_pool::Pool* pool) const
{
    autil::CountedMultiValueType<T> value;
    bool ret = Read(docId, value, pool);
    if (!ret)
    {
        IE_LOG(ERROR, "read value from docId [%d] fail!", docId);
        return false;
    }
    
    uint32_t size = value.size();
    attrValue.clear();
    bool isBinary = false;
    if (this->mAttrConfig)
    {
        isBinary = this->mAttrConfig->GetFieldConfig()->IsBinary();
    }
    if (isBinary)
    {
        // only multi string
        autil::DataBuffer dataBuffer;
        dataBuffer.write(size);
        for (uint32_t i= 0; i < size; i++)
        {
            std::string item = autil::StringUtil::toString<T>(value[i]);
            dataBuffer.write(item);
        }        
        attrValue.assign(dataBuffer.getData(), dataBuffer.getDataLen());
    }
    else
    {
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
    return true;
}

template<typename T>
inline bool VarNumAttributeReader<T>::Read(
        docid_t docId, uint8_t* buf, uint32_t bufLen) const
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentInfos.size(); ++i)
    {
        const index_base::SegmentInfo& segInfo = mSegmentInfos[i];
        if (docId < baseDocId + (docid_t)segInfo.docCount)
        {
            return mSegmentReaders[i]->Read(docId - baseDocId, buf, bufLen);
        }
        baseDocId += segInfo.docCount;
    }
    
    size_t buildingSegIdx = 0;
    return mBuildingAttributeReader && mBuildingAttributeReader->Read(
            docId, buf, bufLen, buildingSegIdx);
}

template<typename T>
inline bool VarNumAttributeReader<T>::Read(docid_t docId,
        autil::CountedMultiValueType<T>& value,
        autil::mem_pool::Pool* pool) const
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentInfos.size(); ++i)
    {
        const index_base::SegmentInfo& segInfo = mSegmentInfos[i];
        if (docId < baseDocId + (docid_t)segInfo.docCount)
        {
            return mSegmentReaders[i]->Read(docId - baseDocId, value, pool);
        }
        baseDocId += segInfo.docCount;
    }

    size_t buildingSegIdx = 0;
    return mBuildingAttributeReader &&
        mBuildingAttributeReader->Read(docId, value, buildingSegIdx, pool);
}

template<typename T>
inline bool VarNumAttributeReader<T>::Read(docid_t docId,
        autil::MultiValueType<T>& value,
        autil::mem_pool::Pool* pool)  const
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentInfos.size(); ++i)
    {
        const index_base::SegmentInfo& segInfo = mSegmentInfos[i];
        if (docId < baseDocId + (docid_t)segInfo.docCount)
        {
            return mSegmentReaders[i]->Read(docId - baseDocId, value, pool);
        }
        baseDocId += segInfo.docCount;
    }

    size_t buildingSegIdx = 0;
    return mBuildingAttributeReader &&
        mBuildingAttributeReader->Read(docId, value, buildingSegIdx, pool);
}

template<typename T>
typename VarNumAttributeReader<T>::AttributeIterator* VarNumAttributeReader<T>::CreateIterator(
        autil::mem_pool::Pool *pool) const
{
    AttributeIterator* attrIt = IE_POOL_COMPATIBLE_NEW_CLASS(pool, 
            AttributeIterator, mSegmentReaders, 
            mBuildingAttributeReader, mSegmentInfos, mBuildingBaseDocId, pool);
    return attrIt;
}

template<typename T>
bool VarNumAttributeReader<T>::UpdateField(docid_t docId, 
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

    if (mBuildingAttributeReader)
    {
        // TODO: mBuildingSegmentReader support updateField
        return mBuildingAttributeReader->UpdateField(docId, buf, bufLen);
    }
    return false;
}

template <typename T>
AttributeSegmentReaderPtr VarNumAttributeReader<T>::GetSegmentReader(
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

template<typename T>
inline void VarNumAttributeReader<T>::InitBuildingAttributeReader(
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
typedef VarNumAttributeReader<int8_t> Int8MultiValueAttributeReader;
DEFINE_SHARED_PTR(Int8MultiValueAttributeReader);
typedef VarNumAttributeReader<uint8_t> UInt8MultiValueAttributeReader;
DEFINE_SHARED_PTR(UInt8MultiValueAttributeReader);
typedef VarNumAttributeReader<int16_t> Int16MultiValueAttributeReader;
DEFINE_SHARED_PTR(Int16MultiValueAttributeReader);
typedef VarNumAttributeReader<uint16_t> UInt16MultiValueAttributeReader;
DEFINE_SHARED_PTR(UInt16MultiValueAttributeReader);
typedef VarNumAttributeReader<int32_t> Int32MultiValueAttributeReader;
DEFINE_SHARED_PTR(Int32MultiValueAttributeReader);
typedef VarNumAttributeReader<uint32_t> UInt32MultiValueAttributeReader;
DEFINE_SHARED_PTR(UInt32MultiValueAttributeReader);
typedef VarNumAttributeReader<int64_t> Int64MultiValueAttributeReader;
DEFINE_SHARED_PTR(Int64MultiValueAttributeReader);
typedef VarNumAttributeReader<uint64_t> UInt64MultiValueAttributeReader;
DEFINE_SHARED_PTR(UInt64MultiValueAttributeReader);
typedef VarNumAttributeReader<float> FloatMultiValueAttributeReader;
DEFINE_SHARED_PTR(FloatMultiValueAttributeReader);
typedef VarNumAttributeReader<double> DoubleMultiValueAttributeReader;
DEFINE_SHARED_PTR(DoubleMultiValueAttributeReader);
typedef VarNumAttributeReader<autil::MultiChar> MultiStringAttributeReader;
DEFINE_SHARED_PTR(MultiStringAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_READER_H
