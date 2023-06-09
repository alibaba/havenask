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
#ifndef __INDEXLIB_ATTRIBUTE_DATA_SEEKER_H
#define __INDEXLIB_ATTRIBUTE_DATA_SEEKER_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/AttributeValueTypeTraits.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/aux_table_reader_creator.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);

namespace indexlib { namespace partition {

class AttributeDataSeeker
{
public:
    AttributeDataSeeker(autil::mem_pool::Pool* pool) : mPool(pool), mOwnPool(false)
    {
        if (!mPool) {
            mPool = new autil::mem_pool::Pool(10240);
            mOwnPool = true;
        }
    }

    virtual ~AttributeDataSeeker()
    {
        if (mOwnPool) {
            DELETE_AND_SET_NULL(mPool);
        }
    }

public:
    bool Init(const IndexPartitionReaderPtr& partReader, const config::AttributeConfigPtr& attrConfig)
    {
        if (!attrConfig) {
            return false;
        }
        mAttrConfig = attrConfig;
        mFieldType = mAttrConfig->GetFieldType();
        mIsMultiValue = mAttrConfig->IsMultiValue();
        mIsLengthFixed = mAttrConfig->IsLengthFixed();
        return DoInit(partReader, attrConfig);
    }

    virtual bool DoInit(const IndexPartitionReaderPtr& partReader, const config::AttributeConfigPtr& attrConfig) = 0;

    virtual bool Seek(const autil::StringView& key, std::string& value) = 0;

    const config::AttributeConfigPtr& GetAttrConfig() const { return mAttrConfig; }
    FieldType GetFieldType() const { return mFieldType; }
    bool IsMultiValue() const { return mIsMultiValue; }
    bool IsLengthFixed() const { return mIsLengthFixed; }
    autil::mem_pool::Pool* GetPool() const { return mPool; }

protected:
    autil::mem_pool::Pool* mPool;
    config::AttributeConfigPtr mAttrConfig;
    FieldType mFieldType;
    bool mIsMultiValue;
    bool mIsLengthFixed;
    bool mOwnPool;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeDataSeeker);

//////////////////////////////////////////////////////////////////////////////////////////
template <typename T>
class AttributeDataSeekerTyped : public AttributeDataSeeker
{
public:
    AttributeDataSeekerTyped(autil::mem_pool::Pool* pool) : AttributeDataSeeker(pool), mAuxTableReader(NULL) {}

    ~AttributeDataSeekerTyped()
    {
        if (mAuxTableReader) {
            POOL_COMPATIBLE_DELETE_CLASS(mPool, mAuxTableReader);
        }
    }

public:
    bool DoInit(const IndexPartitionReaderPtr& partReader, const config::AttributeConfigPtr& attrConfig) override
    {
        mAuxTableReader = AuxTableReaderCreator::Create<T>(partReader, attrConfig->GetAttrName(), mPool);
        return mAuxTableReader != NULL;
    }

    bool Seek(const autil::StringView& key, std::string& valueStr) override
    {
        T value {};
        bool ret = SeekByString(key, value);
        if (ret) {
            valueStr = index::AttributeValueTypeToString<T>::ToString(value);
        }
        return ret;
    }

    bool SeekByString(const autil::StringView& key, T& value)
    {
        assert(mAuxTableReader);
        return mAuxTableReader->GetValue(key, value);
    }
    bool SeekByNumber(uint64_t key, T& value)
    {
        assert(mAuxTableReader);
        return mAuxTableReader->GetValue(key, value);
    }
    bool SeekByHashKey(uint64_t key, T& value)
    {
        assert(mAuxTableReader);
        return mAuxTableReader->GetValueWithPkHash(key, value);
    }

#define BATCH_SEEK_IMPL(func)                                                                                          \
    values.clear();                                                                                                    \
    values.reserve(keys.size());                                                                                       \
    size_t count = 0;                                                                                                  \
    for (size_t i = 0; i < keys.size(); i++) {                                                                         \
        T value {};                                                                                                    \
        if (func(keys[i], value)) {                                                                                    \
            ++count;                                                                                                   \
        }                                                                                                              \
        values.push_back(value);                                                                                       \
    }                                                                                                                  \
    return count;

    size_t BatchSeekByString(const std::vector<autil::StringView>& keys,
                             std::vector<T>& values) {BATCH_SEEK_IMPL(SeekByString)} size_t
        BatchSeekByNumber(const std::vector<uint64_t>& keys,
                          std::vector<T>& values) {BATCH_SEEK_IMPL(SeekByNumber)} size_t
        BatchSeekByHashKey(const std::vector<uint64_t>& keys, std::vector<T>& values)
    {
        BATCH_SEEK_IMPL(SeekByHashKey)
    }
#undef BATCH_SEEK_IMPL

    template <typename HashType>
    AuxTableReaderHasher<HashType> GetHasher() const
    {
        assert(mAuxTableReader);
        return mAuxTableReader->GetHasher();
    }

private:
    AuxTableReaderTyped<T>* mAuxTableReader;
};
}} // namespace indexlib::partition

#endif //__INDEXLIB_ATTRIBUTE_DATA_SEEKER_H
