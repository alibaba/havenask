#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_WRITER_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_WRITER_H

#include <tr1/memory>
#include <tr1/functional>
#include <unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/radix_tree.h"
#include "indexlib/common/typed_slice_list.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_creator.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_var_num_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_updater.h"
#include "indexlib/index/normal/attribute/equal_value_compress_dumper.h"
#include "indexlib/index/normal/attribute/adaptive_attribute_offset_dumper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_accessor.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/util/hash_string.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_writer.h"


IE_NAMESPACE_BEGIN(index);

template<typename T>
class VarNumAttributeWriter : public AttributeWriter
{
public:
    typedef util::HashMap<uint64_t, uint64_t> EncodeMap;
    typedef std::tr1::shared_ptr<EncodeMap> EncodeMapPtr;

public:
    VarNumAttributeWriter(const config::AttributeConfigPtr& attrConfig);
    
    ~VarNumAttributeWriter() {}

    DECLARE_ATTRIBUTE_WRITER_IDENTIFIER(var_num);
    
public:
    class Creator : public AttributeWriterCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return common::TypeInfo<T>::GetFieldType();
        }

        AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const
        {
            return new VarNumAttributeWriter<T>(attrConfig);
        }
    };

public:
    void Init(const FSWriterParamDeciderPtr& fsWriterParamDecider,
              util::BuildResourceMetrics* buildResourceMetrics) override; 
    void AddField(docid_t docId, 
                  const autil::ConstString& attributeValue) override;
    bool UpdateField(docid_t docId, 
                     const autil::ConstString& attributeValue) override;

    const AttributeSegmentReaderPtr CreateInMemReader() const override;
    void Dump(const file_system::DirectoryPtr &dir,
              autil::mem_pool::PoolBase* dumpPool) override;

protected:
    void AddEncodeField(docid_t docId, 
                        const autil::ConstString& attributeValue);
    void AddNormalField(docid_t docId, 
                        const autil::ConstString& attributeValue);

    void DumpOffsetFile(
            const file_system::DirectoryPtr &attrDir, 
            AdaptiveAttributeOffsetDumper& offsetDumper)
    {
        assert(mFsWriterParamDecider);
        const file_system::FileWriterPtr offsetFile = 
            attrDir->CreateFileWriter(
                ATTRIBUTE_OFFSET_FILE_NAME,
                mFsWriterParamDecider->MakeParam(ATTRIBUTE_OFFSET_FILE_NAME));

        std::string offsetFilePath = offsetFile->GetPath();
        IE_LOG(DEBUG, "Start dumping attribute to offset file : %s",
               offsetFilePath.c_str());
        if (offsetDumper.Size() > 0)
        {
            offsetDumper.Dump(offsetFile);
        }
        offsetFile->Close();
        IE_LOG(DEBUG, "Finish dumping attribute to offset file : %s",
               offsetFilePath.c_str());
    }

    uint64_t AppendAttribute(const common::AttrValueMeta& meta);

    void DumpFile(const file_system::DirectoryPtr &attrDir, 
                  const std::string &fileName, 
                  const VarNumAttributeAccessor* data,
                  AdaptiveAttributeOffsetDumper& offsetDumper,
                  autil::mem_pool::PoolBase* dumpPool);
private:
    void UpdateBuildResourceMetrics() override;
    uint64_t InitDumpEstimateFactor() const;    
    
protected:
    EncodeMapPtr mEncodeMap;
    VarNumAttributeAccessor* mAccessor;
    uint64_t mDumpEstimateFactor;
    bool mIsUniqEncode;
    bool mIsOffsetFileCopyOnDump;

private:
    friend class VarNumAttributeWriterTest;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index,VarNumAttributeWriter);

//////////////////////////////
template<typename T>
VarNumAttributeWriter<T>::VarNumAttributeWriter(
        const config::AttributeConfigPtr& attrConfig)
    : AttributeWriter(attrConfig)
    , mAccessor(NULL)
    , mDumpEstimateFactor(0)
    , mIsUniqEncode(false)
    , mIsOffsetFileCopyOnDump(false)
{
}

template <typename T>
inline void VarNumAttributeWriter<T>::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode)
    {
        return;
    }
    int64_t poolSize = mPool->getUsedBytes() + mSimplePool.getUsedBytes();
    // dump file size = data file size + max offset file size
    size_t docCount = mAccessor->GetDocCount();
    int64_t dumpOffsetFileSize = sizeof(uint64_t) * docCount;
    int64_t dumpFileSize = mAccessor->GetAppendDataSize() + dumpOffsetFileSize;
    int64_t dumpTempBufferSize = mDumpEstimateFactor * docCount;
    if (mIsOffsetFileCopyOnDump)
    {
        dumpTempBufferSize += dumpOffsetFileSize;
    }
    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempBufferSize);
    // do not report zero value metrics for performance consideration
    // mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, 0);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

template<typename T>
inline void VarNumAttributeWriter<T>::Init(
        const FSWriterParamDeciderPtr& fsWriterParamDecider,
        util::BuildResourceMetrics* buildResourceMetrics)
{
    AttributeWriter::Init(fsWriterParamDecider, buildResourceMetrics);
    assert(mPool);
    char* buffer = (char*)mPool->allocate(sizeof(VarNumAttributeAccessor));
    mAccessor = new (buffer) VarNumAttributeAccessor();
    mAccessor->Init(mPool.get());
    mIsUniqEncode = mAttrConfig->IsUniqEncode();
    mEncodeMap.reset(new EncodeMap(mPool.get(), HASHMAP_INIT_SIZE));
    mDumpEstimateFactor = InitDumpEstimateFactor();
    mIsOffsetFileCopyOnDump =
        mFsWriterParamDecider->MakeParam(ATTRIBUTE_OFFSET_FILE_NAME).copyOnDump;
    UpdateBuildResourceMetrics();
}

template<typename T>
inline void VarNumAttributeWriter<T>::AddField(docid_t docId, 
        const autil::ConstString& attributeValue)
{
    assert(mAttrConvertor);
    common::AttrValueMeta meta;
    std::string emptyAttrValue;
    meta = mAttrConvertor->Decode(attributeValue);
    if (mIsUniqEncode)
    {
        mAccessor->AddEncodedField(meta, mEncodeMap);
    }
    else
    {
        mAccessor->AddNormalField(meta);
    }
    UpdateBuildResourceMetrics();
}

template<typename T>
inline bool VarNumAttributeWriter<T>::UpdateField(docid_t docId,
        const autil::ConstString& attributeValue)
{
    common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
    bool retFlag = false;
    if (mIsUniqEncode)
    {
        retFlag = mAccessor->UpdateEncodedField(docId, meta, mEncodeMap);
    }
    else
    {
        retFlag = mAccessor->UpdateNormalField(docId, meta);
    }
    UpdateBuildResourceMetrics();
    return retFlag;
}

template<typename T>
inline void VarNumAttributeWriter<T>::DumpFile(
        const file_system::DirectoryPtr &attrDir, const std::string &fileName, 
        const VarNumAttributeAccessor* data,
        AdaptiveAttributeOffsetDumper& offsetDumper,
        autil::mem_pool::PoolBase* dumpPool)
{
    assert(offsetDumper.Size() == 0);
    assert(mFsWriterParamDecider);
    const file_system::FileWriterPtr dataFile = 
        attrDir->CreateFileWriter(
            fileName, mFsWriterParamDecider->MakeParam(fileName));
    
    dataFile->ReserveFileNode(data->GetAppendDataSize());
    std::string dataFilePath = dataFile->GetPath();
    IE_LOG(DEBUG, "Start dumping attribute to data file : %s",
           dataFilePath.c_str());

    //offsetMap to support uniq multi value
    typedef autil::mem_pool::pool_allocator<std::pair<const uint64_t, uint64_t> > AllocatorType;
    typedef std::unordered_map<uint64_t, uint64_t,
                               std::tr1::hash<uint64_t>,
                               std::equal_to<uint64_t>,
                               AllocatorType> OffsetMap;
    bool needOffsetMap = mAttrConfig->IsUniqEncode();
    AllocatorType allocator(dumpPool);
    OffsetMap offsetMap(10, OffsetMap::hasher(), OffsetMap::key_equal(), allocator);

    VarNumAttributeDataIteratorPtr iter = 
        data->CreateVarNumAttributeDataIterator();

    uint32_t dataItemCount = 0;
    uint64_t currentOffset = 0;
    uint64_t maxItemLen = 0;
    while (iter->HasNext())
    {
        uint64_t offset = 0;
        uint8_t* data = NULL;
        uint64_t dataLength = 0;

        iter->Next();
        iter->GetCurrentOffset(offset);

        if (!needOffsetMap)
        {
            iter->GetCurrentData(dataLength, data);
            dataFile->Write(data, dataLength);
            ++dataItemCount;
            maxItemLen = std::max(maxItemLen, dataLength);
            offsetDumper.PushBack(currentOffset);
            currentOffset += dataLength;
            continue;
        }

        OffsetMap::const_iterator offsetIter = offsetMap.find(offset);
        if (offsetIter != offsetMap.end())
        {
            offsetDumper.PushBack(offsetIter->second);
        }
        else
        {
            iter->GetCurrentData(dataLength, data);
            dataFile->Write(data, dataLength);
            maxItemLen = std::max(maxItemLen, dataLength);
            ++dataItemCount;
            offsetDumper.PushBack(currentOffset);
            offsetMap.insert(std::make_pair(offset, currentOffset));
            currentOffset += dataLength;
        }
    }

    //add last offset to guard
    offsetDumper.PushBack(currentOffset);

    assert(dataFile->GetLength() <= data->GetAppendDataSize());
    dataFile->Close();
    IE_LOG(DEBUG, "Finish dumping attribute to data file : %s",
           dataFilePath.c_str());

    AttributeDataInfo dataInfo(dataItemCount, maxItemLen);
    attrDir->Store(ATTRIBUTE_DATA_INFO_FILE_NAME, dataInfo.ToString(), false);
    IE_LOG(DEBUG, "Finish Store attribute data info");
}

template<typename T>
inline void VarNumAttributeWriter<T>::Dump(const file_system::DirectoryPtr &dir,
        autil::mem_pool::PoolBase* dumpPool)
{
    std::string attributeName = mAttrConfig->GetAttrName();
    IE_LOG(DEBUG, "Dumping attribute : [%s]", attributeName.c_str());

    AdaptiveAttributeOffsetDumper offsetDumper(dumpPool);
    offsetDumper.Init(mAttrConfig);
    offsetDumper.Reserve(mAccessor->GetDocCount() + 1);

    file_system::DirectoryPtr attrDir = 
        dir->MakeDirectory(attributeName);

    DumpFile(attrDir, ATTRIBUTE_DATA_FILE_NAME, mAccessor, offsetDumper, dumpPool);
    DumpOffsetFile(attrDir, offsetDumper);
    UpdateBuildResourceMetrics();
}

template <typename T>
const AttributeSegmentReaderPtr VarNumAttributeWriter<T>::CreateInMemReader() const
{
    InMemVarNumAttributeReader<T> * pReader = 
        new InMemVarNumAttributeReader<T>(mAccessor,
                mAttrConfig->GetCompressType(),
                mAttrConfig->GetFieldConfig()->GetFixedMultiValueCount());
    return AttributeSegmentReaderPtr(pReader);
}

template<typename T>
uint64_t VarNumAttributeWriter<T>::InitDumpEstimateFactor() const
{
    double factor = 0;
    config::CompressTypeOption compressType = mAttrConfig->GetCompressType();
    if (compressType.HasEquivalentCompress())
    {
        // use max offset type
        factor += sizeof(uint64_t) * 2;
    }

    if (compressType.HasUniqEncodeCompress())
    {
        // hash map for uniq comress dump
        factor += 2 * sizeof(std::pair<uint64_t, uint64_t>);
    }
    // adaptive offset
    factor += sizeof(uint32_t);
    return factor;
}

typedef VarNumAttributeWriter<int8_t> Int8MultiValueAttributeWriter;
DEFINE_SHARED_PTR(Int8MultiValueAttributeWriter);
typedef VarNumAttributeWriter<uint8_t> UInt8MultiValueAttributeWriter;
DEFINE_SHARED_PTR(UInt8MultiValueAttributeWriter);
typedef VarNumAttributeWriter<int16_t> Int16MultiValueAttributeWriter;
DEFINE_SHARED_PTR(Int16MultiValueAttributeWriter);
typedef VarNumAttributeWriter<uint16_t> UInt16MultiValueAttributeWriter;
DEFINE_SHARED_PTR(UInt16MultiValueAttributeWriter);
typedef VarNumAttributeWriter<int32_t> Int32MultiValueAttributeWriter;
DEFINE_SHARED_PTR(Int32MultiValueAttributeWriter);
typedef VarNumAttributeWriter<uint32_t> UInt32MultiValueAttributeWriter;
DEFINE_SHARED_PTR(UInt32MultiValueAttributeWriter);
typedef VarNumAttributeWriter<int64_t> Int64MultiValueAttributeWriter;
DEFINE_SHARED_PTR(Int64MultiValueAttributeWriter);
typedef VarNumAttributeWriter<uint64_t> UInt64MultiValueAttributeWriter;
DEFINE_SHARED_PTR(UInt64MultiValueAttributeWriter);
typedef VarNumAttributeWriter<float> FloatMultiValueAttributeWriter;
DEFINE_SHARED_PTR(FloatMultiValueAttributeWriter);
typedef VarNumAttributeWriter<double> DoubleMultiValueAttributeWriter;
DEFINE_SHARED_PTR(DoubleMultiValueAttributeWriter);


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_WRITER_H
