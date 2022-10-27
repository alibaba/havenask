#ifndef __INDEXLIB_ATTRIBUTE_DATA_PATCHER_H
#define __INDEXLIB_ATTRIBUTE_DATA_PATCHER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"

DECLARE_REFERENCE_CLASS(partition, AttributePatchDataWriter);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(storage, IOConfig);

IE_NAMESPACE_BEGIN(partition);

class AttributeDataPatcher
{
public:
    AttributeDataPatcher();
    virtual ~AttributeDataPatcher();

public:
    bool Init(const config::AttributeConfigPtr& attrConfig,
              const storage::IOConfig& mergeIOConfig,
              const std::string& segmentDirPath, uint32_t docCount);

    void AppendFieldValue(const std::string& valueStr);
    void AppendFieldValue(const autil::ConstString& valueStr);
    void AppendFieldValueFromRawIndex(const autil::ConstString& rawIndexValue);
    
    virtual void AppendEncodedValue(const autil::ConstString& value) = 0;
    virtual void Close() = 0;

    config::AttributeConfigPtr GetAttrConfig() const { return mAttrConf; }
    uint32_t GetPatchedDocCount() const { return mPatchedDocCount; }
    uint32_t GetTotalDocCount() const { return mTotalDocCount; }
public:
    // only for ut
    AttributePatchDataWriterPtr TEST_GetAttributePatchDataWriter()
    {
        return mPatchDataWriter;
    }
protected:
    virtual bool DoInit(const storage::IOConfig& mergeIOConfig) = 0;

    // ensure earlier encoded value not used after reset pool
    void ReleasePoolMemoryIfNeed()
    {
        if (mPool.getUsedBytes() < POOL_CHUNK_SIZE)
        {
            return;
        }
        mPool.release();
    }

    void AppendAllDocByDefaultValue();

protected:
    common::AttributeConvertorPtr mAttrConvertor;
    AttributePatchDataWriterPtr mPatchDataWriter;
    uint32_t mTotalDocCount;
    uint32_t mPatchedDocCount;
    autil::mem_pool::Pool mPool;
    config::AttributeConfigPtr mAttrConf;
    std::string mDirPath;

    static const size_t POOL_CHUNK_SIZE = 10 * 1024 * 1024;

protected:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeDataPatcher);

inline void AttributeDataPatcher::AppendFieldValue(const autil::ConstString& valueStr) {
    ReleasePoolMemoryIfNeed();
    assert(mAttrConvertor);
    autil::ConstString encodeValue = mAttrConvertor->Encode(valueStr, &mPool);
    AppendEncodedValue(encodeValue);
}

inline void AttributeDataPatcher::AppendFieldValueFromRawIndex(
        const autil::ConstString& rawIndexValue) {
    ReleasePoolMemoryIfNeed();
    assert(mAttrConvertor);
    autil::ConstString encodeValue = mAttrConvertor->EncodeFromRawIndexValue(
            rawIndexValue, &mPool);
    AppendEncodedValue(encodeValue);
}

////////////////////////////////////////////////////////////////////////

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ATTRIBUTE_DATA_PATCHER_H
