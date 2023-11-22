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

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(partition, AttributePatchDataWriter);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

namespace indexlib { namespace partition {

class AttributeDataPatcher
{
public:
    AttributeDataPatcher();
    virtual ~AttributeDataPatcher();

public:
    bool Init(const config::AttributeConfigPtr& attrConfig, const config::MergeIOConfig& mergeIOConfig,
              const file_system::DirectoryPtr& segmentDir, uint32_t docCount);

    void AppendFieldValue(const std::string& valueStr);
    void EncodeFieldValue(const autil::StringView& inputStr, autil::StringView& encodedValue,
                          autil::mem_pool::Pool& pool);
    void AppendFieldValue(const autil::StringView& valueStr);
    void AppendFieldValueFromRawIndex(const autil::StringView& rawIndexValue, bool isNull = false);

    void AppendNullValue();
    virtual void AppendEncodedValue(const autil::StringView& value) = 0;
    virtual void Close() = 0;

    config::AttributeConfigPtr GetAttrConfig() const { return mAttrConf; }
    uint32_t GetPatchedDocCount() const { return mPatchedDocCount; }
    uint32_t GetTotalDocCount() const { return mTotalDocCount; }

public:
    // only for ut
    AttributePatchDataWriterPtr TEST_GetAttributePatchDataWriter() { return mPatchDataWriter; }

protected:
    virtual bool DoInit(const config::MergeIOConfig& mergeIOConfig) = 0;

    // ensure earlier encoded value not used after reset pool
    void ReleasePoolMemoryIfNeed()
    {
        if (mPool.getUsedBytes() < POOL_CHUNK_SIZE) {
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
    file_system::DirectoryPtr mDirectory;
    std::string mNullString;
    bool mSupportNull;

    static const size_t POOL_CHUNK_SIZE = 10 * 1024 * 1024;

protected:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeDataPatcher);

inline void AttributeDataPatcher::AppendFieldValue(const autil::StringView& valueStr)
{
    ReleasePoolMemoryIfNeed();
    if (mAttrConf && mSupportNull && valueStr == mNullString) {
        AppendNullValue();
        return;
    }

    autil::StringView encodeValue;
    EncodeFieldValue(valueStr, encodeValue, mPool);
    AppendEncodedValue(encodeValue);
}

inline void AttributeDataPatcher::EncodeFieldValue(const autil::StringView& inputStr, autil::StringView& encodedValue,
                                                   autil::mem_pool::Pool& pool)
{
    if (mAttrConf && mSupportNull && inputStr == mNullString) {
        encodedValue = autil::MakeCString(inputStr, &pool);
        return;
    }
    assert(mAttrConvertor);
    encodedValue = mAttrConvertor->Encode(inputStr, &pool);
    return;
}

inline void AttributeDataPatcher::AppendFieldValueFromRawIndex(const autil::StringView& rawIndexValue, bool isNull)
{
    ReleasePoolMemoryIfNeed();
    if (mAttrConf && mSupportNull && isNull) {
        AppendNullValue();
        return;
    }

    assert(mAttrConvertor);
    autil::StringView encodeValue = mAttrConvertor->EncodeFromRawIndexValue(rawIndexValue, &mPool);
    AppendEncodedValue(encodeValue);
}

////////////////////////////////////////////////////////////////////////
}} // namespace indexlib::partition
