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

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Define.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/index/DocMapDumpParams.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/format/SingleEncodedNullValue.h"
#include "indexlib/index/common/FileCompressParamHelper.h"
#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"
#include "indexlib/index/common/data_structure/EqualValueCompressDumper.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/slice_array/ByteAlignedSliceArray.h"

namespace indexlibv2::index {

class SingleValueAttributeMemFormatterBase : private autil::NoCopyable
{
};

template <typename T>
class SingleValueAttributeMemFormatter : public SingleValueAttributeMemFormatterBase
{
public:
    SingleValueAttributeMemFormatter(const std::shared_ptr<AttributeConfig>& attrConfig);
    ~SingleValueAttributeMemFormatter() = default;

private:
    const static uint32_t SLICE_LEN = 16 * 1024;
    const static uint32_t SLOT_NUM = 64;

public:
    void Init(std::shared_ptr<autil::mem_pool::Pool> pool, const std::shared_ptr<AttributeConvertor>& attrConvertor);
    void AddField(docid_t docId, const T& value, bool isNull);
    void AddField(docid_t docId, const autil::StringView& attributeValue, bool isNull);

    bool UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull);
    Status Dump(const std::shared_ptr<indexlib::file_system::Directory>& dir, autil::mem_pool::PoolBase* dumpPool,
                const std::shared_ptr<framework::DumpParams>& dumpParams);

    inline bool Read(docid_t docId, T& value, bool& isNull) const __ALWAYS_INLINE;

    int64_t GetDumpFileSize() const;
    uint64_t GetDocCount() const;

public:
    indexlib::index::TypedSliceListBase* TEST_GetAttributeData() const { return _data; }

private:
    Status DumpFile(const std::shared_ptr<indexlib::file_system::Directory>& dir, const std::string& fileName,
                    autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<framework::DumpParams>& dumpParams);
    Status DumpUncompressedFile(const std::shared_ptr<indexlib::file_system::FileWriter>& dataFile,
                                std::vector<docid_t>* new2old);
    template <bool SupportNull, bool IsSortDump>
    Status DumpUncompressedFileImpl(const std::shared_ptr<indexlib::file_system::FileWriter>& dataFile,
                                    std::vector<docid_t>* new2old);

private:
    std::shared_ptr<AttributeConfig> _attrConfig;
    bool _supportNull;
    T _encodedNullValue;
    indexlib::index::TypedSliceList<T>* _data;
    indexlib::index::TypedSliceList<uint64_t>* _nullFieldBitmap;
    std::shared_ptr<AttributeConvertor> _attrConvertor;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeMemFormatter, T);

template <typename T>
SingleValueAttributeMemFormatter<T>::SingleValueAttributeMemFormatter(
    const std::shared_ptr<AttributeConfig>& attrConfig)
    : _attrConfig(attrConfig)
    , _data(NULL)
    , _nullFieldBitmap(NULL)
{
    _supportNull = attrConfig->GetFieldConfig()->IsEnableNullField();
    SingleEncodedNullValue::GetEncodedValue<T>((void*)&_encodedNullValue);
}

template <typename T>
void SingleValueAttributeMemFormatter<T>::AddField(docid_t docId, const autil::StringView& attributeValue, bool isNull)
{
    if (attributeValue.empty()) {
        T value = 0;
        AddField(docId, value, isNull);
        return;
    }
    assert(_attrConvertor);
    AttrValueMeta meta = _attrConvertor->Decode(attributeValue);
    assert(meta.data.size() == sizeof(T));
    AddField(docId, *(T*)meta.data.data(), isNull);
}

template <typename T>
void SingleValueAttributeMemFormatter<T>::AddField(docid_t docId, const T& value, bool isNull)
{
    if (!_supportNull) {
        _data->PushBack(value);
        return;
    }
    uint64_t bitmapId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    if (bitmapId + 1 > _nullFieldBitmap->Size()) {
        _nullFieldBitmap->PushBack(0);
    }
    if (isNull) {
        int ref = docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
        uint64_t* bitMap = NULL;
        _nullFieldBitmap->Read(bitmapId, bitMap);
        (*bitMap) = (*bitMap) | (1UL << ref);
        _data->PushBack(_encodedNullValue);
    } else {
        _data->PushBack(value);
    }
}

template <typename T>
bool SingleValueAttributeMemFormatter<T>::UpdateField(docid_t docId, const autil::StringView& attributeValue,
                                                      bool isNull)
{
    assert(_data);
    if ((uint64_t)(docId + 1) > _data->Size()) {
        AUTIL_LOG(ERROR, "can not update attribute for not-exist docId[%d]", docId);
        ERROR_COLLECTOR_LOG(ERROR, "can not update attribute for not-exist docId[%d]", docId);
        return false;
    }
    // update not null field
    if (!_supportNull) {
        assert(_attrConvertor);
        AttrValueMeta meta = _attrConvertor->Decode(attributeValue);
        _data->Update(docId, *((T*)meta.data.data()));
        return true;
    }

    // update null field
    int ref = docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    uint64_t* bitMap = NULL;
    _nullFieldBitmap->Read(docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE, bitMap);
    if (isNull) {
        // update bitmap first, then update value
        (*bitMap) = (*bitMap) | (1UL << ref);
        T* value = NULL;
        _data->Read(docId, value);
        memcpy(value, &_encodedNullValue, sizeof(T));
        return true;
    }
    // update value first, then update bitmap
    assert(_attrConvertor);
    AttrValueMeta meta = _attrConvertor->Decode(attributeValue);
    _data->Update(docId, *((T*)meta.data.data()));
    (*bitMap) = (*bitMap) & (~(1UL << ref));
    return true;
}

template <typename T>
Status SingleValueAttributeMemFormatter<T>::Dump(const std::shared_ptr<indexlib::file_system::Directory>& directory,
                                                 autil::mem_pool::PoolBase* dumpPool,
                                                 const std::shared_ptr<framework::DumpParams>& dumpParams)
{
    std::string attributeName = _attrConfig->GetAttrName();
    AUTIL_LOG(DEBUG, "Dumping attribute : [%s]", attributeName.c_str());

    auto [st, subDir] =
        directory->GetIDirectory()->MakeDirectory(attributeName, indexlib::file_system::DirectoryOption()).StatusWith();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "make subdir [%s] fail, ErrorInfo: [%s]. ", attributeName.c_str(), st.ToString().c_str());
        return st;
    }
    return DumpFile(indexlib::file_system::IDirectory::ToLegacyDirectory(subDir), ATTRIBUTE_DATA_FILE_NAME, dumpPool,
                    dumpParams);
}

template <typename T>
Status SingleValueAttributeMemFormatter<T>::DumpFile(const std::shared_ptr<indexlib::file_system::Directory>& dir,
                                                     const std::string& fileName, autil::mem_pool::PoolBase* dumpPool,
                                                     const std::shared_ptr<framework::DumpParams>& dumpParams)
{
    indexlib::file_system::WriterOption option;
    FileCompressParamHelper::SyncParam(_attrConfig->GetFileCompressConfigV2(), nullptr, option);
    auto [status, fileWriter] = dir->GetIDirectory()->CreateFileWriter(fileName, option).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "faile to create file writer for [%s], ErrorInfo: [%s]. ", fileName.c_str(),
                  status.ToString().c_str());
        return status;
    }
    auto params = std::dynamic_pointer_cast<DocMapDumpParams>(dumpParams);
    std::vector<docid_t>* new2old = params ? &params->new2old : nullptr;
    AUTIL_LOG(DEBUG, "Start dumping attribute to data file : %s", fileWriter->DebugString().c_str());
    if (AttributeCompressInfo::NeedCompressData(_attrConfig)) {
        EqualValueCompressDumper<T> dumper(false, dumpPool);
        dumper.CompressData(_data, new2old);
        status = dumper.Dump(fileWriter);
    } else {
        status = DumpUncompressedFile(fileWriter, new2old);
    }
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "fail to dump , ErrorInfo: [%s]", status.ToString().c_str());
        return status;
    }
    status = fileWriter->Close().Status();
    AUTIL_LOG(DEBUG, "Finish dumping attribute to data file : %s", fileWriter->DebugString().c_str());
    return status;
}

template <typename T>
Status SingleValueAttributeMemFormatter<T>::DumpUncompressedFile(
    const std::shared_ptr<indexlib::file_system::FileWriter>& dataFile, std::vector<docid_t>* new2old)
{
    if (_supportNull) {
        return new2old ? DumpUncompressedFileImpl<true, true>(dataFile, new2old)
                       : DumpUncompressedFileImpl<true, false>(dataFile, new2old);
    } else {
        return new2old ? DumpUncompressedFileImpl<false, true>(dataFile, new2old)
                       : DumpUncompressedFileImpl<false, false>(dataFile, new2old);
    }
}

template <typename T>
template <bool SupportNull, bool IsSortDump>
Status SingleValueAttributeMemFormatter<T>::DumpUncompressedFileImpl(
    const std::shared_ptr<indexlib::file_system::FileWriter>& dataFile, std::vector<docid_t>* new2old)
{
    assert(!new2old || _data->Size() == new2old->size());
    for (size_t i = 0; i < _data->Size(); ++i) {
        if constexpr (SupportNull) {
            if (i % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE == 0) {
                uint64_t nullValue = 0;
                if constexpr (IsSortDump) {
                    for (size_t j = i; j < _data->Size() && j < i + SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
                         ++j) {
                        docid_t docId = new2old->at(j);
                        int ref = docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
                        uint64_t* bitMap = NULL;
                        _nullFieldBitmap->Read(docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE, bitMap);
                        if ((*bitMap) & (1UL << ref)) {
                            nullValue |= (1UL << (j % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE));
                        }
                    }
                } else {
                    _nullFieldBitmap->Read(i / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE, nullValue);
                }
                auto [writeSt, len] = dataFile->Write((char*)&nullValue, sizeof(uint64_t)).StatusWith();
                if (!writeSt.IsOK()) {
                    AUTIL_LOG(ERROR, "fail to dump , ErrorInfo: [%s]", writeSt.ToString().c_str());
                    return writeSt;
                }
            }
        }

        docid_t docId = i;
        if constexpr (IsSortDump) {
            docId = new2old->at(i);
        }
        T* value = NULL;
        _data->Read(docId, value);
        auto [writeSt, len] = dataFile->Write((char*)value, sizeof(T)).StatusWith();
        if (!writeSt.IsOK()) {
            AUTIL_LOG(ERROR, "fail to dump , ErrorInfo: [%s]", writeSt.ToString().c_str());
            return writeSt;
        }
    }
    return Status::OK();
}

template <typename T>
inline bool SingleValueAttributeMemFormatter<T>::Read(docid_t docId, T& value, bool& isNull) const
{
    assert(_data);
    _data->Read(docId, value);
    if (!_supportNull || value != _encodedNullValue) {
        isNull = false;
        return true;
    }
    int ref = docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    uint64_t* bitMap = NULL;
    _nullFieldBitmap->Read(docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE, bitMap);
    isNull = (*bitMap) & (1UL << ref);
    return true;
}

template <typename T>
void SingleValueAttributeMemFormatter<T>::Init(std::shared_ptr<autil::mem_pool::Pool> pool,
                                               const std::shared_ptr<AttributeConvertor>& attrConvertor)
{
    assert(pool);
    void* buffer = pool->allocate(sizeof(indexlib::index::TypedSliceList<T>));
    _data = new (buffer) indexlib::index::TypedSliceList<T>(SLOT_NUM, SLICE_LEN, pool.get());
    if (_supportNull) {
        void* bitmapBuffer = pool->allocate(sizeof(indexlib::index::TypedSliceList<uint64_t>));
        _nullFieldBitmap =
            new (bitmapBuffer) indexlib::index::TypedSliceList<uint64_t>(SLOT_NUM, SLICE_LEN, pool.get());
    }
    _attrConvertor = attrConvertor;
}

template <typename T>
int64_t SingleValueAttributeMemFormatter<T>::GetDumpFileSize() const
{
    return _supportNull ? _data->Size() * sizeof(T) + _nullFieldBitmap->Size() * sizeof(uint64_t)
                        : _data->Size() * sizeof(T);
}

template <typename T>
uint64_t SingleValueAttributeMemFormatter<T>::GetDocCount() const
{
    assert(_data != NULL);
    return _data->Size();
}

} // namespace indexlibv2::index
