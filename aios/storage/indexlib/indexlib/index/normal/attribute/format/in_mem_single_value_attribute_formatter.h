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

#include "autil/ConstString.h"
#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/index/common/DefaultFSWriterParamDecider.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"
#include "indexlib/index/data_structure/equal_value_compress_dumper.h"
#include "indexlib/index/normal/attribute/format/single_value_null_attr_formatter.h"
#include "indexlib/index/util/file_compress_param_helper.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/slice_array/ByteAlignedSliceArray.h"

namespace indexlib { namespace index {

class InMemSingleValueAttributeFormatterBase
{
};

DEFINE_SHARED_PTR(InMemSingleValueAttributeFormatterBase);

template <typename T>
class InMemSingleValueAttributeFormatter : public InMemSingleValueAttributeFormatterBase
{
public:
    InMemSingleValueAttributeFormatter(const config::AttributeConfigPtr& attrConfig)
        : mAttrConfig(attrConfig)
        , mData(NULL)
        , mNullFieldBitmap(NULL)
    {
        mSupportNull = attrConfig->GetFieldConfig()->IsEnableNullField();
        SingleEncodedNullValue::GetEncodedValue<T>((void*)&mEncodedNullValue);
    }
    ~InMemSingleValueAttributeFormatter() {}

private:
    const static uint32_t SLICE_LEN = 16 * 1024;
    const static uint32_t SLOT_NUM = 64;
    typedef std::shared_ptr<autil::mem_pool::Pool> PoolPtr;

public:
    void Init(const FSWriterParamDeciderPtr& fsWriterParamDecider, PoolPtr pool)
    {
        mFsWriterParamDecider = fsWriterParamDecider;
        assert(pool);
        void* buffer = pool->allocate(sizeof(TypedSliceList<T>));
        mData = new (buffer) TypedSliceList<T>(SLOT_NUM, SLICE_LEN, pool.get());
        if (mSupportNull) {
            void* bitmapBuffer = pool->allocate(sizeof(TypedSliceList<uint64_t>));
            mNullFieldBitmap = new (bitmapBuffer) TypedSliceList<uint64_t>(SLOT_NUM, SLICE_LEN, pool.get());
        }
    }

    void SetAttrConvertor(const common::AttributeConvertorPtr& attrConvertor) { mAttrConvertor = attrConvertor; }

    // for case
    TypedSliceListBase* GetAttributeData() { return mData; }

public:
    // for writer & reader
    void AddField(docid_t docId, const T& value, bool isNull);
    void AddField(docid_t docId, const autil::StringView& attributeValue, bool isNull);

    bool UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull);
    void Dump(const file_system::DirectoryPtr& dir, const std::string& temperatureLayer,
              autil::mem_pool::PoolBase* dumpPool);

    void DumpFile(const file_system::DirectoryPtr& dir, const std::string& fileName,
                  const std::string& temperatureLayer, autil::mem_pool::PoolBase* dumpPool);

    int64_t GetDumpFileSize()
    {
        return mSupportNull ? mData->Size() * sizeof(T) + mNullFieldBitmap->Size() * sizeof(uint64_t)
                            : mData->Size() * sizeof(T);
    }

    bool Read(docid_t docId, T& value, bool& isNull) const;
    uint64_t GetDocCount() const
    {
        assert(mData != NULL);
        return mData->Size();
    }

private:
    void DumpUncompressedFile(const file_system::FileWriterPtr& dataFile);

private:
    config::AttributeConfigPtr mAttrConfig;
    bool mSupportNull;
    T mEncodedNullValue;
    TypedSliceList<T>* mData;
    TypedSliceList<uint64_t>* mNullFieldBitmap;
    FSWriterParamDeciderPtr mFsWriterParamDecider;
    common::AttributeConvertorPtr mAttrConvertor;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, InMemSingleValueAttributeFormatter);
//// inline function
template <typename T>
inline void InMemSingleValueAttributeFormatter<T>::AddField(docid_t docId, const autil::StringView& attributeValue,
                                                            bool isNull)
{
    if (attributeValue.empty()) {
        T value = 0;
        AddField(docId, value, isNull);
        return;
    }
    assert(mAttrConvertor);
    common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
    assert(meta.data.size() == sizeof(T));
    AddField(docId, *(T*)meta.data.data(), isNull);
}

template <typename T>
inline void InMemSingleValueAttributeFormatter<T>::AddField(docid_t docId, const T& value, bool isNull)
{
    if (!mSupportNull) {
        mData->PushBack(value);
        return;
    }
    uint64_t bitmapId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    if (bitmapId + 1 > mNullFieldBitmap->Size()) {
        mNullFieldBitmap->PushBack(0);
    }
    if (isNull) {
        int ref = docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
        uint64_t* bitMap = NULL;
        mNullFieldBitmap->Read(bitmapId, bitMap);
        (*bitMap) = (*bitMap) | (1UL << ref);
        mData->PushBack(mEncodedNullValue);
    } else {
        mData->PushBack(value);
    }
}

template <typename T>
inline bool InMemSingleValueAttributeFormatter<T>::UpdateField(docid_t docId, const autil::StringView& attributeValue,
                                                               bool isNull)
{
    assert(mData);
    if ((uint64_t)(docId + 1) > mData->Size()) {
        IE_LOG(ERROR, "can not update attribute for not-exist docId[%d]", docId);
        ERROR_COLLECTOR_LOG(ERROR, "can not update attribute for not-exist docId[%d]", docId);
        return false;
    }
    // update not null field
    if (!mSupportNull) {
        assert(mAttrConvertor);
        common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
        mData->Update(docId, *((T*)meta.data.data()));
        return true;
    }

    // update null field
    int ref = docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    uint64_t* bitMap = NULL;
    mNullFieldBitmap->Read(docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE, bitMap);
    if (isNull) {
        // update bitmap first, then update value
        (*bitMap) = (*bitMap) | (1UL << ref);
        T* value = NULL;
        mData->Read(docId, value);
        memcpy(value, &mEncodedNullValue, sizeof(T));
        return true;
    }
    // update value first, then update bitmap
    assert(mAttrConvertor);
    common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
    mData->Update(docId, *((T*)meta.data.data()));
    (*bitMap) = (*bitMap) & (~(1UL << ref));
    return true;
}

template <typename T>
inline void InMemSingleValueAttributeFormatter<T>::Dump(const file_system::DirectoryPtr& directory,
                                                        const std::string& temperatureLayer,
                                                        autil::mem_pool::PoolBase* dumpPool)
{
    std::string attributeName = mAttrConfig->GetAttrName();
    IE_LOG(DEBUG, "Dumping attribute : [%s]", attributeName.c_str());
    file_system::DirectoryPtr subDir = directory->MakeDirectory(mAttrConfig->GetAttrName());
    DumpFile(subDir, ATTRIBUTE_DATA_FILE_NAME, temperatureLayer, dumpPool);
}

template <typename T>
inline void InMemSingleValueAttributeFormatter<T>::DumpFile(const file_system::DirectoryPtr& dir,
                                                            const std::string& fileName,
                                                            const std::string& temperatureLayer,
                                                            autil::mem_pool::PoolBase* dumpPool)
{
    assert(mFsWriterParamDecider);
    auto option = mFsWriterParamDecider->MakeParam(fileName);
    FileCompressParamHelper::SyncParam(mAttrConfig->GetFileCompressConfig(), temperatureLayer, option);
    file_system::FileWriterPtr fileWriter = dir->CreateFileWriter(fileName, option);
    IE_LOG(DEBUG, "Start dumping attribute to data file : %s", fileWriter->DebugString().c_str());
    if (AttributeCompressInfo::NeedCompressData(mAttrConfig)) {
        EqualValueCompressDumper<T> dumper(false, dumpPool);
        dumper.CompressData(mData, nullptr);
        dumper.Dump(fileWriter);
    } else {
        DumpUncompressedFile(fileWriter);
    }
    fileWriter->Close().GetOrThrow();
    IE_LOG(DEBUG, "Finish dumping attribute to data file : %s", fileWriter->DebugString().c_str());
}

template <typename T>
inline void InMemSingleValueAttributeFormatter<T>::DumpUncompressedFile(const file_system::FileWriterPtr& dataFile)
{
    dataFile->ReserveFile(sizeof(T) * mData->Size()).GetOrThrow();
    if (!mSupportNull) {
        for (uint32_t i = 0; i < mData->Size(); ++i) {
            T* value = NULL;
            mData->Read(i, value);
            dataFile->Write((char*)value, sizeof(T)).GetOrThrow();
        }
        return;
    }
    // for enable_null field
    for (uint32_t i = 0; i < mData->Size(); ++i) {
        if (i % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE == 0) {
            uint64_t* nullValue = NULL;
            mNullFieldBitmap->Read(i / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE, nullValue);
            dataFile->Write((char*)nullValue, sizeof(uint64_t)).GetOrThrow();
        }
        T* value = NULL;
        mData->Read(i, value);
        dataFile->Write((char*)value, sizeof(T)).GetOrThrow();
    }
}

template <typename T>
inline bool __ALWAYS_INLINE InMemSingleValueAttributeFormatter<T>::Read(docid_t docId, T& value, bool& isNull) const
{
    assert(mData);
    mData->Read(docId, value);
    if (!mSupportNull || value != mEncodedNullValue) {
        isNull = false;
        return true;
    }

    int ref = docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    uint64_t* bitMap = NULL;
    mNullFieldBitmap->Read(docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE, bitMap);
    isNull = (*bitMap) & (1UL << ref);
    return true;
}
}} // namespace indexlib::index
