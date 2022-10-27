#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_WRITER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_WRITER_H

#include <tr1/memory>
#include <fslib/fs/File.h>
#include <autil/LongHashValue.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_creator.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/index/normal/attribute/equal_value_compress_dumper.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"
#include "indexlib/util/simple_pool.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class SingleValueAttributeWriter : public AttributeWriter
{
public:
    SingleValueAttributeWriter(const config::AttributeConfigPtr& attrConfig)
        : AttributeWriter(attrConfig)
        , mData(NULL)
        , mIsDataFileCopyOnDump(false)
    {
    }
    
    ~SingleValueAttributeWriter() {}

    DECLARE_ATTRIBUTE_WRITER_IDENTIFIER(single);

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
            return new SingleValueAttributeWriter<T>(attrConfig);
        }
    };

public:
    void Init(const FSWriterParamDeciderPtr& fsWriterParamDecider,
                             util::BuildResourceMetrics* buildResourceMetrics) override;

    void AddField(docid_t docId,
                                 const autil::ConstString& attributeValue) override;

    bool UpdateField(docid_t docId,
                                    const autil::ConstString& attributeValue) override;

    void Dump(const file_system::DirectoryPtr& dir,
                             autil::mem_pool::PoolBase* dumpPool) override;

    inline const AttributeSegmentReaderPtr CreateInMemReader() const override;

    void AddField(docid_t docId, const T& value);    

    // dump data to dir, no mkdir sub attr name dir
    // user create attr dir
    void DumpData(const file_system::DirectoryPtr& dir)
    {
        util::SimplePool pool;
        DumpFile(dir, ATTRIBUTE_DATA_FILE_NAME, mData, &pool);
    }

protected:
    void DumpFile(const file_system::DirectoryPtr& dir,
                  const std::string &fileName, 
                  const common::TypedSliceList<T>* data,
                  autil::mem_pool::PoolBase* dumpPool);

    void DumpUncompressedFile(
            const common::TypedSliceList<T>* data,
            const file_system::FileWriterPtr& dataFile);

    void DumpCompressedFile(
            const common::TypedSliceList<T>* data,
            const file_system::FileWriterPtr& dataFile,
            autil::mem_pool::PoolBase* dumpPool);
private:
    friend class SingleValueAttributeWriterTest;
    
    common::TypedSliceList<T>* GetAttributeData() const { return mData; }
    void UpdateBuildResourceMetrics() override;

private:
    const static uint32_t SLICE_LEN  = 16 * 1024;
    const static uint32_t SLOT_NUM  = 64;
    common::TypedSliceList<T>* mData;
    bool mIsDataFileCopyOnDump;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeWriter);

/////////////////////////////////////////////////////////
// inline functions

template <typename T>
inline void SingleValueAttributeWriter<T>::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode)
    {
        return;
    }
    int64_t poolSize = mPool->getUsedBytes() + mSimplePool.getUsedBytes();
    int64_t dumpFileSize = mData->Size() * sizeof(T);
    int64_t dumpTempBufferSize = 0;
    if (mAttrConfig->GetCompressType().HasEquivalentCompress())
    {
        // compressor need double buffer
        // worst case : no compress at all
        dumpTempBufferSize = dumpFileSize * 2;
    }
    if (mIsDataFileCopyOnDump)
    {
        dumpTempBufferSize += dumpFileSize;
    }
    
    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempBufferSize);
    // do not report zero value metrics for performance consideration
    // mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, 0);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

template<typename T>
inline void SingleValueAttributeWriter<T>::Init(
        const FSWriterParamDeciderPtr& fsWriterParamDecider,
        util::BuildResourceMetrics* buildResourceMetrics)
{
    AttributeWriter::Init(fsWriterParamDecider, buildResourceMetrics);
    assert(mPool);
    void* buffer = mPool->allocate(sizeof(common::TypedSliceList<T>));
    mData = new (buffer) common::TypedSliceList<T>(SLOT_NUM, SLICE_LEN, mPool.get());
    mIsDataFileCopyOnDump =
        mFsWriterParamDecider->MakeParam(ATTRIBUTE_DATA_FILE_NAME).copyOnDump;
    UpdateBuildResourceMetrics();
}

template<typename T>
inline void SingleValueAttributeWriter<T>::AddField(docid_t docId, 
        const autil::ConstString& attributeValue)
{
    if (attributeValue.empty())
    {
        T value = 0;
        AddField(docId, value);
        return;
    }
    common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
    assert(meta.data.size() == sizeof(T));
    AddField(docId, *(T*)meta.data.data());
}

//add field must inorder and no loss docid
template<typename T>
inline void SingleValueAttributeWriter<T>::AddField(docid_t docId,
        const T& value)
{
    mData->PushBack(value);
    UpdateBuildResourceMetrics();    
}

template<typename T>
inline bool SingleValueAttributeWriter<T>::UpdateField(docid_t docId, 
        const autil::ConstString& attributeValue)
{
    if ((uint64_t)(docId + 1) > mData->Size())
    {
        IE_LOG(ERROR, "can not update attribute for not-exist docId[%d]", docId);
        ERROR_COLLECTOR_LOG(ERROR, "can not update attribute for not-exist docId[%d]",
                            docId);
        return false;
    }
    common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
    mData->Update(docId, *((T*)meta.data.data()));
    UpdateBuildResourceMetrics(); 
    return true;
}

template<typename T>
inline void SingleValueAttributeWriter<T>::Dump(
        const file_system::DirectoryPtr& directory,
        autil::mem_pool::PoolBase* dumpPool)
{
    std::string attributeName = mAttrConfig->GetAttrName();
    IE_LOG(DEBUG, "Dumping attribute : [%s]", attributeName.c_str());
    file_system::DirectoryPtr subDir = 
        directory->MakeDirectory(mAttrConfig->GetAttrName());
    DumpFile(subDir, ATTRIBUTE_DATA_FILE_NAME, mData, dumpPool);
    UpdateBuildResourceMetrics();    
}

template<typename T>
inline const AttributeSegmentReaderPtr SingleValueAttributeWriter<T>::CreateInMemReader() const
{
    return AttributeSegmentReaderPtr(new InMemSingleValueAttributeReader<T>(mData));
}

template<>
inline const AttributeSegmentReaderPtr SingleValueAttributeWriter<int16_t>::CreateInMemReader() const
{
    const config::CompressTypeOption& compress = mAttrConfig->GetFieldConfig()->GetCompressType();
    if (compress.HasFp16EncodeCompress())
    {
        return AttributeSegmentReaderPtr(new InMemSingleValueAttributeReader<float>(mData, compress));
    }
    return AttributeSegmentReaderPtr(new InMemSingleValueAttributeReader<int16_t>(mData));
}

template<>
inline const AttributeSegmentReaderPtr SingleValueAttributeWriter<int8_t>::CreateInMemReader() const
{
    const config::CompressTypeOption& compress = mAttrConfig->GetFieldConfig()->GetCompressType();
    if (compress.HasInt8EncodeCompress())
    {
        return AttributeSegmentReaderPtr(new InMemSingleValueAttributeReader<float>(mData, compress));
    }
    return AttributeSegmentReaderPtr(new InMemSingleValueAttributeReader<int8_t>(mData));
}

template<typename T>
inline void SingleValueAttributeWriter<T>::DumpUncompressedFile(
        const common::TypedSliceList<T>* data, 
        const file_system::FileWriterPtr& dataFile)
{
    dataFile->ReserveFileNode(sizeof(T) * data->Size());

    for (uint32_t i = 0; i < data->Size(); ++i)
    {
        T value;
        data->Read(i, value);
        dataFile->Write((char*)&value, sizeof(T));
    }
    assert(dataFile->GetLength() == sizeof(T) * data->Size());
}

template<typename T>
inline void SingleValueAttributeWriter<T>::DumpCompressedFile(
        const common::TypedSliceList<T>* data,
        const file_system::FileWriterPtr& dataFile,
        autil::mem_pool::PoolBase* dumpPool)
{
    EqualValueCompressDumper<T> dumper(false, dumpPool);
    dumper.CompressData(data);
    dumper.Dump(dataFile);
}

template<typename T>
inline void SingleValueAttributeWriter<T>::DumpFile(
        const file_system::DirectoryPtr& dir,
        const std::string &fileName,
        const common::TypedSliceList<T>* data,
        autil::mem_pool::PoolBase* dumpPool)
{
    assert(mFsWriterParamDecider);
    const file_system::FileWriterPtr file = dir->CreateFileWriter(
        fileName, mFsWriterParamDecider->MakeParam(fileName));

    std::string filePath = file->GetPath();
    IE_LOG(DEBUG, "Start dumping attribute to data file : %s",
           filePath.c_str());
    
    if (AttributeCompressInfo::NeedCompressData(mAttrConfig))
    {
        DumpCompressedFile(data, file, dumpPool);
    }
    else
    {
        DumpUncompressedFile(data, file);
    }
    file->Close();
    IE_LOG(DEBUG, "Finish dumping attribute to data file : %s",
           filePath.c_str());
}


typedef SingleValueAttributeWriter<float> FloatAttributeWriter;
DEFINE_SHARED_PTR(FloatAttributeWriter);

typedef SingleValueAttributeWriter<double> DoubleAttributeWriter;
DEFINE_SHARED_PTR(DoubleAttributeWriter);

typedef SingleValueAttributeWriter<int64_t> Int64AttributeWriter;
DEFINE_SHARED_PTR(Int64AttributeWriter);

typedef SingleValueAttributeWriter<uint64_t> UInt64AttributeWriter;
DEFINE_SHARED_PTR(UInt64AttributeWriter);

typedef SingleValueAttributeWriter<uint64_t> Hash64AttributeWriter;
DEFINE_SHARED_PTR(Hash64AttributeWriter);

typedef SingleValueAttributeWriter<autil::uint128_t> UInt128AttributeWriter;
DEFINE_SHARED_PTR(UInt128AttributeWriter);

typedef SingleValueAttributeWriter<autil::uint128_t> Hash128AttributeWriter;
DEFINE_SHARED_PTR(Hash128AttributeWriter);

typedef SingleValueAttributeWriter<int32_t> Int32AttributeWriter;
DEFINE_SHARED_PTR(Int32AttributeWriter);

typedef SingleValueAttributeWriter<uint32_t> UInt32AttributeWriter;
DEFINE_SHARED_PTR(UInt32AttributeWriter);

typedef SingleValueAttributeWriter<int16_t> Int16AttributeWriter;
DEFINE_SHARED_PTR(Int16AttributeWriter);

typedef SingleValueAttributeWriter<uint16_t> UInt16AttributeWriter;
DEFINE_SHARED_PTR(UInt16AttributeWriter);

typedef SingleValueAttributeWriter<int8_t> Int8AttributeWriter;
DEFINE_SHARED_PTR(Int8AttributeWriter);

typedef SingleValueAttributeWriter<uint8_t> UInt8AttributeWriter;
DEFINE_SHARED_PTR(UInt8AttributeWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_WRITER_H
