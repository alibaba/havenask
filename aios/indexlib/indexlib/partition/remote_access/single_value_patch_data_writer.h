#ifndef __INDEXLIB_SINGLE_VALUE_PATCH_DATA_WRITER_H
#define __INDEXLIB_SINGLE_VALUE_PATCH_DATA_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/index/normal/attribute/equal_value_compress_dumper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/util/simple_pool.h"
#include "indexlib/util/path_util.h"

DECLARE_REFERENCE_CLASS(index, AttributeConvertor);

IE_NAMESPACE_BEGIN(partition);

template <typename T>
class SingleValuePatchDataWriter : public AttributePatchDataWriter
{
public:
    SingleValuePatchDataWriter()
        : mBufferCursor(0)
        , mBufferSize(0)
        , mDataBuffer(NULL)
        , mRecordSize(0)
    {}
    
    ~SingleValuePatchDataWriter()
    {
        mCompressDumper.reset();
        mPool.reset();
        DeleteBuffer();
    }
    
public:
    bool Init(const config::AttributeConfigPtr& attrConfig,
              const config::MergeIOConfig& mergeIOConfig,
              const std::string& dirPath) override;
    
    void AppendValue(const autil::ConstString& value) override;

    void Close() override;
    file_system::FileWriterPtr TEST_GetDataFileWriter() override
    { return mDataFile; }
private:
    void FlushDataBuffer();
    void DeleteBuffer()
    {
        if (mDataBuffer)
        {
            delete[] mDataBuffer;
            mDataBuffer = NULL;
            mBufferSize = 0;
            mBufferCursor = 0;
        }
    }
    
private:
    static const uint32_t DEFAULT_RECORD_COUNT = 1024 * 1024;
    typedef index::EqualValueCompressDumper<T> EqualValueCompressDumper;
    DEFINE_SHARED_PTR(EqualValueCompressDumper);
    
private:
    uint32_t mBufferCursor;
    uint32_t mBufferSize;
    uint8_t* mDataBuffer;
    uint32_t mRecordSize;

    common::AttributeConvertorPtr mAttrConvertor;
    file_system::FileWriterPtr mDataFile;
    EqualValueCompressDumperPtr mCompressDumper;
    std::tr1::shared_ptr<autil::mem_pool::PoolBase> mPool;
    
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(partition, SingleValuePatchDataWriter);

/////////////////////////////////////////////////////////////////////////////////
template <typename T>
inline bool SingleValuePatchDataWriter<T>::Init(
        const config::AttributeConfigPtr& attrConfig,
        const config::MergeIOConfig& mergeIOConfig,
        const std::string& dirPath)
{
    if (index::AttributeCompressInfo::NeedCompressData(attrConfig))
    {
        mPool.reset(new util::SimplePool);
        mCompressDumper.reset(new EqualValueCompressDumper(false, mPool.get()));
    }
    
    mRecordSize = sizeof(T);
    mBufferSize = DEFAULT_RECORD_COUNT * mRecordSize;
    mDataBuffer = new uint8_t[mBufferSize];
    mBufferCursor = 0;

    std::string dataFilePath =
        util::PathUtil::JoinPath(dirPath, ATTRIBUTE_DATA_FILE_NAME);
    mDataFile.reset(CreateBufferedFileWriter(mergeIOConfig.writeBufferSize,
                                             mergeIOConfig.enableAsyncWrite));
    mDataFile->Open(dataFilePath);

    mAttrConvertor.reset(
            common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                    attrConfig->GetFieldConfig()));
    if (!mAttrConvertor)
    {
        IE_LOG(ERROR, "create attribute convertor fail!");
        return false;
    }
    return true;
}

template <typename T>
inline void SingleValuePatchDataWriter<T>::AppendValue(const autil::ConstString& encodeValue)
{
    assert(mAttrConvertor);
    common::AttrValueMeta meta = mAttrConvertor->Decode(encodeValue);
    const autil::ConstString& value = meta.data;
    if (value.size() != mRecordSize)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "value length [%u] not match with record size [%u]",
                             (uint32_t)value.size(), mRecordSize);
    }

    memcpy(mDataBuffer + mBufferCursor, value.data(), mRecordSize);
    mBufferCursor += mRecordSize;
    if (mBufferCursor == mBufferSize)
    {
        FlushDataBuffer();
    }
}

template <typename T>
inline void SingleValuePatchDataWriter<T>::Close()
{
    FlushDataBuffer();
    if (mCompressDumper && mDataFile)
    {
        mCompressDumper->Dump(mDataFile);
        mCompressDumper->Reset();
    }
    
    if (mDataFile)
    {
        mDataFile->Close();
    }
    DeleteBuffer();
}

template <typename T>
inline void SingleValuePatchDataWriter<T>::FlushDataBuffer()
{
    if (mBufferCursor == 0)
    {
        return;
    }
    if (mCompressDumper)
    {
        mCompressDumper->CompressData((T*)mDataBuffer, mBufferCursor / mRecordSize);
    }
    else
    {
        assert(mDataFile);
        mDataFile->Write(mDataBuffer, mBufferCursor);
    }
    mBufferCursor = 0;
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SINGLE_VALUE_PATCH_DATA_WRITER_H
