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
#include <fcntl.h>
#include <memory>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "autil/MultiValueFormatter.h"
#include "autil/TimeUtility.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_offset_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/patch_apply_option.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_defrag_slice_array.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/format/updatable_var_num_attribute_offset_formatter.h"
#include "indexlib/index/normal/attribute/format/var_num_attribute_data_formatter.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class MultiValueAttributeSegmentReader : public AttributeSegmentReader
{
public:
    MultiValueAttributeSegmentReader(const config::AttributeConfigPtr& config,
                                     AttributeMetrics* attributeMetrics = NULL, TemperatureProperty property = UNKNOWN);

    ~MultiValueAttributeSegmentReader() {}
    struct ReadContext : public ReadContextBase {
        ReadContext(AttributeOffsetReader&& offsetReader, const std::shared_ptr<file_system::FileStream>& fileStream,
                    autil::mem_pool::Pool* pool)
            : offsetReader(std::move(offsetReader))
            , fileStream(fileStream)
            , pool(pool)
        {
        }
        ReadContext& operator=(ReadContext&&) = default;
        ReadContext(ReadContext&&) = default;
        ReadContext& operator=(const ReadContext&) = delete;
        ReadContext(const ReadContext&) = delete;
        AttributeOffsetReader offsetReader;
        std::shared_ptr<file_system::FileStream> fileStream;
        autil::mem_pool::Pool* pool;
    };

public:
    bool IsInMemory() const override { return false; }
    uint32_t TEST_GetDataLength(docid_t docId) const override;
    bool Read(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf, uint32_t bufLen, bool& isNull) override;
    ReadContextBasePtr CreateReadContextPtr(autil::mem_pool::Pool* pool) const override
    {
        return ReadContextBasePtr(new ReadContext(CreateReadContext(pool)));
    }
    bool ReadDataAndLen(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf, uint32_t bufLen,
                        uint32_t& dataLen) override;
    uint64_t GetOffset(docid_t docId, const ReadContextBasePtr& ctx) const override;
    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull) override;
    bool Updatable() const override { return mUpdatble; }

public:
    void Open(const index_base::SegmentData& segData, const PatchApplyOption& patchApplyOption,
              const file_system::DirectoryPtr& attrDirectory, const MultiValueAttributeSegmentReader* hintReader,
              bool disableUpdate);

    inline bool Read(docid_t docId, autil::MultiValueType<T>& value, bool& isNull,
                     ReadContext& ctx) const __ALWAYS_INLINE;

    future_lite::coro::Lazy<index::ErrorCodeVec> BatchRead(const std::vector<docid_t>& docIds, ReadContext& ctx,
                                                           file_system::ReadOption readOption,
                                                           typename std::vector<autil::MultiValueType<T>>* values,
                                                           std::vector<bool>* isNullVec) const noexcept;

    ReadContext CreateReadContext(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        ReadContext ctx(mOffsetReader.CreateSessionReader(sessionPool),
                        mData ? nullptr : mFileStream->CreateSessionStream(sessionPool), sessionPool);
        return ctx;
    }

    uint8_t* GetBaseAddress() const { return mData; }
    uint32_t GetMaxDataItemLen() const
    {
        uint32_t ret = mDataInfo.maxItemLen;
        if (mPatchReader && mPatchReader->GetMaxPatchItemLen() > ret) {
            ret = mPatchReader->GetMaxPatchItemLen();
        }
        return ret;
    }

    void FetchValueFromStreamNoCopy(uint64_t offset, const std::shared_ptr<file_system::FileStream>& fileStream,
                                    uint8_t* buffer, uint32_t& dataLen, bool& isNull) const;

private:
    void InitSliceFileReader(AttributeOffsetReader* offsetReader, const file_system::DirectoryPtr& attrDirectory,
                             VarNumAttributeDataFormatter dataFormatter,
                             UpdatableVarNumAttributeOffsetFormatter offsetFormatter);
    bool ReadToBuffer(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf, uint32_t bufLen, uint32_t& dataLen,
                      bool& isNull) const __ALWAYS_INLINE;

    file_system::FileReaderPtr CreateFileReader(const file_system::DirectoryPtr& directory, const std::string& fileName,
                                                bool supportFileCompress);
    const uint8_t* FetchValueFromStream(uint64_t offset, const std::shared_ptr<file_system::FileStream>& fileStream,
                                        autil::mem_pool::Pool* pool) const;

    const uint8_t* ReadFromPatch(docid_t docId, autil::mem_pool::Pool* pool) const;

    const uint8_t* ReadData(docid_t docId, ReadContext& ctx) const __ALWAYS_INLINE;
    void ReadCount(const std::shared_ptr<file_system::FileStream>& fileStream, uint8_t* buffPtr, uint64_t offset,
                   size_t& encodeCountLen, uint32_t& count, bool& isNull) const;
    future_lite::coro::Lazy<std::vector<index::Result<uint8_t*>>>
    BatchReadData(const std::vector<docid_t>& docIds, ReadContext& ctx,
                  file_system::ReadOption readOption) const noexcept;

protected:
    uint32_t mDocCount;
    int32_t mFixedValueCount;
    UpdatableVarNumAttributeOffsetFormatter mOffsetFormatter;
    uint8_t* mData;
    VarNumAttributeDefragSliceArrayPtr mDefragSliceArray;
    config::CompressTypeOption mCompressType;

    PatchApplyStrategy mPatchApplyStrategy;
    std::shared_ptr<VarNumAttributePatchReader<T>> mPatchReader;
    std::shared_ptr<file_system::FileStream> mFileStream;
    AttributeOffsetReader mOffsetReader;
    VarNumAttributeDataFormatter mDataFormatter;
    file_system::FileReaderPtr mSliceFile;
    config::AttributeConfigPtr mAttrConfig;
    AttributeDataInfo mDataInfo;
    std::string mNullValue;
    bool mSupportNull;
    bool mUpdatble;

    static const uint32_t RESERVE_SLICE_NUM = 8 * 1024;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, MultiValueAttributeSegmentReader);

template <typename T>
struct MultiValueAttributeSegmentReaderWithCtx {
    std::shared_ptr<MultiValueAttributeSegmentReader<T>> reader;
    AttributeSegmentReader::ReadContextBasePtr ctx;
};

//////////////////////////////////////////////////////
template <typename T>
MultiValueAttributeSegmentReader<T>::MultiValueAttributeSegmentReader(const config::AttributeConfigPtr& config,
                                                                      AttributeMetrics* attributeMetrics,
                                                                      TemperatureProperty property)
    : AttributeSegmentReader(attributeMetrics, property)
    , mDocCount(0)
    , mFixedValueCount(config->GetFieldConfig()->GetFixedMultiValueCount())
    , mData(nullptr)
    , mCompressType(config->GetCompressType())
    , mPatchApplyStrategy(PatchApplyStrategy::PAS_APPLY_UNKNOW)
    , mOffsetReader(config, attributeMetrics)
    , mAttrConfig(config)
    , mSupportNull(config->SupportNull())
    , mUpdatble(false)
{
    assert(config);

    common::VarNumAttributeConvertor<T> convertor;
    mNullValue = convertor.EncodeNullValue();
}

template <typename T>
inline void MultiValueAttributeSegmentReader<T>::Open(const index_base::SegmentData& segData,
                                                      const PatchApplyOption& patchApplyOption,
                                                      const file_system::DirectoryPtr& attrDirectory,
                                                      const MultiValueAttributeSegmentReader* hintReader,
                                                      bool disableUpdate)
{
    autil::ScopedTime2 timer;
    std::string attrName = mAttrConfig->GetAttrName();
    IE_LOG(INFO, "start loading segment [%d] for attribute [%s]... ", segData.GetSegmentId(), attrName.c_str());
    mPatchApplyStrategy = patchApplyOption.applyStrategy;

    bool supportFileCompress = SupportFileCompress(mAttrConfig, *segData.GetSegmentInfo());
    file_system::DirectoryPtr baseDirectory = attrDirectory;
    if (!baseDirectory) {
        baseDirectory = segData.GetAttributeDirectory(attrName, true);
    }
    file_system::FileReaderPtr dataFile;
    if (hintReader) {
        mDataInfo = hintReader->mDataInfo;
        mFileStream = hintReader->mFileStream;
        mData = hintReader->mData;
    } else {
        // open data info file
        std::string dataInfoStr;
        baseDirectory->Load(ATTRIBUTE_DATA_INFO_FILE_NAME, dataInfoStr);
        mDataInfo.FromString(dataInfoStr);
        dataFile = CreateFileReader(baseDirectory, ATTRIBUTE_DATA_FILE_NAME, supportFileCompress);
        mFileStream = indexlib::file_system::FileStreamCreator::CreateConcurrencyFileStream(dataFile, nullptr);
        mData = (uint8_t*)dataFile->GetBaseAddress();
    }
    mDocCount = segData.GetSegmentInfo()->docCount;
    mOffsetReader.Init(baseDirectory, mDocCount, supportFileCompress, disableUpdate);
    mOffsetFormatter.Init(mFileStream->GetStreamLength());
    mDataFormatter.Init(mAttrConfig);

    if (!disableUpdate && mAttrConfig->IsAttributeUpdatable() && mData && mOffsetReader.IsSupportUpdate()) {
        InitSliceFileReader(&mOffsetReader, baseDirectory, mDataFormatter, mOffsetFormatter);
        mUpdatble = true;
    } else {
        mDefragSliceArray = VarNumAttributeDefragSliceArrayPtr();
        mUpdatble = false;
    }

    if (mPatchApplyStrategy == PatchApplyStrategy::PAS_APPLY_ON_READ) {
        mPatchReader = DYNAMIC_POINTER_CAST(VarNumAttributePatchReader<T>, patchApplyOption.patchReader);
    } else {
        assert(mPatchApplyStrategy == PatchApplyStrategy::PAS_APPLY_NO_PATCH);
        assert(!patchApplyOption.patchReader);
    }

    IE_LOG(INFO, "Finishing loading segment(%d) for attribute(%s), updatble[%d], used[%.3f]s",
           (int32_t)segData.GetSegmentId(), attrName.c_str(), mUpdatble, timer.done_sec());
}

template <typename T>
void MultiValueAttributeSegmentReader<T>::InitSliceFileReader(AttributeOffsetReader* offsetReader,
                                                              const file_system::DirectoryPtr& attrDirectory,
                                                              VarNumAttributeDataFormatter dataFormatter,
                                                              UpdatableVarNumAttributeOffsetFormatter offsetFormatter)
{
    mSliceFile = attrDirectory->CreateFileReader(ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME, file_system::FSOT_SLICE);
    if (!mSliceFile) {
        auto fileWriter = attrDirectory->CreateFileWriter(
            ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME,
            file_system::WriterOption::Slice(common::VarNumAttributeFormatter::VAR_NUM_ATTRIBUTE_SLICE_LEN,
                                             RESERVE_SLICE_NUM));
        mSliceFile = attrDirectory->CreateFileReader(ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME, file_system::FSOT_SLICE);
        fileWriter->Close().GetOrThrow();
    }
    auto sliceFileReader = DYNAMIC_POINTER_CAST(file_system::SliceFileReader, mSliceFile);
    const util::BytesAlignedSliceArrayPtr& sliceArray = sliceFileReader->GetBytesAlignedSliceArray();
    VarNumAttributeDefragSliceArrayPtr defragSliceArray(new VarNumAttributeDefragSliceArray(
        sliceArray, mAttrConfig->GetAttrName(), mAttrConfig->GetDefragSlicePercent()));
    defragSliceArray->Init(offsetReader, offsetFormatter, dataFormatter, mAttributeMetrics);
    mDefragSliceArray = defragSliceArray;
}

template <typename T>
inline file_system::FileReaderPtr
MultiValueAttributeSegmentReader<T>::CreateFileReader(const file_system::DirectoryPtr& directory,
                                                      const std::string& fileName, bool supportFileCompress)
{
    file_system::ReaderOption option(file_system::FSOT_LOAD_CONFIG);
    option.supportCompress = supportFileCompress;
    return directory->CreateFileReader(fileName, option);
}

template <typename T>
inline const uint8_t* MultiValueAttributeSegmentReader<T>::FetchValueFromStream(
    uint64_t offset, const std::shared_ptr<file_system::FileStream>& fileStream, autil::mem_pool::Pool* pool) const
{
    assert(pool);
    if (!pool) {
        return nullptr;
    }
    bool isNull = false;
    size_t dataLength = mDataFormatter.GetDataLengthFromStream(fileStream, offset, isNull);
    char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, char, dataLength);
    fileStream->Read(buffer, dataLength, offset, file_system::ReadOption::LowLatency()).GetOrThrow();
    return (const uint8_t*)buffer;
}

template <typename T>
inline void MultiValueAttributeSegmentReader<T>::FetchValueFromStreamNoCopy(
    uint64_t offset, const std::shared_ptr<file_system::FileStream>& fileStream, uint8_t* buf, uint32_t& dataLen,
    bool& isNull) const
{
    assert(fileStream);
    dataLen = mDataFormatter.GetDataLengthFromStream(fileStream, offset, isNull);
    size_t retLen = fileStream->Read(buf, dataLen, offset, file_system::ReadOption()).GetOrThrow();
    (void)retLen;
}

template <typename T>
inline void MultiValueAttributeSegmentReader<T>::ReadCount(const std::shared_ptr<file_system::FileStream>& fileStream,
                                                           uint8_t* buffPtr, uint64_t offset, size_t& encodeCountLen,
                                                           uint32_t& count, bool& isNull) const
{
    if (mFixedValueCount != -1) {
        count = (uint32_t)mFixedValueCount;
        encodeCountLen = 0;
        return;
    }

    size_t retLen = fileStream->Read(buffPtr, sizeof(uint8_t), offset, file_system::ReadOption()).GetOrThrow();
    (void)retLen;
    encodeCountLen = common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*buffPtr);
    // read remain bytes for count
    retLen =
        fileStream
            ->Read(buffPtr + sizeof(uint8_t), encodeCountLen - 1, offset + sizeof(uint8_t), file_system::ReadOption())
            .GetOrThrow();

    isNull = false;
    count = common::VarNumAttributeFormatter::DecodeCount((const char*)buffPtr, encodeCountLen, isNull);
}

template <typename T>
const uint8_t* MultiValueAttributeSegmentReader<T>::ReadFromPatch(docid_t docId, autil::mem_pool::Pool* pool) const
{
    auto maxLen = mPatchReader->GetMaxPatchItemLen();
    uint8_t* data = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, uint8_t, maxLen);
    if (mPatchReader->Seek(docId, data, maxLen)) {
        return data;
    }
    IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, data, maxLen);
    return nullptr;
}

template <typename T>
inline const uint8_t* MultiValueAttributeSegmentReader<T>::ReadData(docid_t docId, ReadContext& ctx) const
{
    if (unlikely(mPatchApplyStrategy == PatchApplyStrategy::PAS_APPLY_ON_READ && mPatchReader)) {
        const uint8_t* data = ReadFromPatch(docId, ctx.pool);
        if (data) {
            return data;
        }
    }
    assert(docId >= 0 && docId < (docid_t)mDocCount);
    uint64_t offset = ctx.offsetReader.GetOffset(docId);
    const uint8_t* data = NULL;
    if (!mOffsetFormatter.IsSliceArrayOffset(offset)) {
        if (!mData) {
            data = FetchValueFromStream(offset, ctx.fileStream, ctx.pool);
        } else {
            data = mData + offset;
        }
    } else {
        assert(mDefragSliceArray);
        data = (const uint8_t*)mDefragSliceArray->Get(mOffsetFormatter.DecodeToSliceArrayOffset(offset));
    }
    return data;
}

template <typename T>
inline bool MultiValueAttributeSegmentReader<T>::Read(docid_t docId, autil::MultiValueType<T>& value, bool& isNull,
                                                      ReadContext& ctx) const
{
    if (docId < 0 || docId >= (docid_t)mDocCount) {
        return false;
    }

    const uint8_t* data = ReadData(docId, ctx);
    if (!data) {
        return false;
    }
    if (mEnableAccessCountors && mAttributeMetrics && mProperty != UNKNOWN) {
        mAttributeMetrics->IncAccessCounter(mAttrConfig->GetAttrName(), mProperty, 1);
    }
    if (mFixedValueCount == -1) {
        value.init((const void*)data);
        isNull = mSupportNull ? value.isNull() : false;
        return true;
    } else {
        value = autil::MultiValueType<T>(reinterpret_cast<const char*>(data), mFixedValueCount);
        isNull = false;
        return true;
    }
}

template <typename T>
inline uint64_t MultiValueAttributeSegmentReader<T>::GetOffset(docid_t docId, const ReadContextBasePtr& ctx) const
{
    ReadContext* typedCtx = (ReadContext*)ctx.get();
    return typedCtx->offsetReader.GetOffset(docId);
}

template <typename T>
inline uint32_t MultiValueAttributeSegmentReader<T>::TEST_GetDataLength(docid_t docId) const
{
    ReadContext ctx = CreateReadContext(nullptr);
    const uint8_t* data = ReadData(docId, ctx);
    if (!data) {
        return 0;
    }
    bool isNull = false;
    return mDataFormatter.GetDataLength(data, isNull);
}

template <typename T>
inline bool MultiValueAttributeSegmentReader<T>::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull)
{
    if (isNull) {
        common::VarNumAttributeConvertor<T> convertor;
        common::AttrValueMeta meta = convertor.Decode(autil::StringView(mNullValue));
        buf = (uint8_t*)meta.data.data();
        bufLen = (uint32_t)meta.data.size();
    }

    if (!mUpdatble) {
        return false;
    }

    if (docId < 0 || docId >= (docid_t)mDocCount) {
        return false;
    }
    ReadContext ctx = CreateReadContext(nullptr);
    const uint8_t* data = ReadData(docId, ctx);
    assert(data);
    bool tmpIsNull = false;
    uint32_t size = mDataFormatter.GetDataLength(data, tmpIsNull);
    if (size == bufLen && memcmp(data, buf, bufLen) == 0) {
        return true;
    }

    uint64_t offset = mDefragSliceArray->Append(buf, bufLen);
    uint64_t originalOffset = mOffsetReader.GetOffset(docId);
    mOffsetReader.SetOffset(docId, mOffsetFormatter.EncodeSliceArrayOffset(offset));

    if (mOffsetFormatter.IsSliceArrayOffset(originalOffset)) {
        mDefragSliceArray->Free(mOffsetFormatter.DecodeToSliceArrayOffset(originalOffset), size);
    }
    return true;
}

template <>
inline bool MultiValueAttributeSegmentReader<float>::Read(docid_t docId, autil::MultiValueType<float>& value,
                                                          bool& isNull, ReadContext& ctx) const
{
    if (docId < 0 || docId >= (docid_t)mDocCount) {
        return false;
    }

    const uint8_t* data = ReadData(docId, ctx);
    if (!data) {
        return false;
    }
    if (mEnableAccessCountors && mAttributeMetrics && mProperty != UNKNOWN) {
        mAttributeMetrics->IncAccessCounter(mAttrConfig->GetAttrName(), mProperty, 1);
    }
    if (mFixedValueCount == -1) {
        value.init((const void*)data);
        isNull = mSupportNull ? value.isNull() : false;
        return true;
    }

    if (!ctx.pool) {
        IE_LOG(ERROR, "pool is nullptr when read MultiValueType value from fixed length attribute");
        return false;
    }
    isNull = false;
    common::FloatCompressConvertor convertor(mCompressType, mFixedValueCount);
    return convertor.GetValue((const char*)data, value, ctx.pool);
}

template <typename T>
inline bool MultiValueAttributeSegmentReader<T>::Read(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf,
                                                      uint32_t bufLen, bool& isNull)
{
    uint32_t dataLen;
    return ReadToBuffer(docId, ctx, buf, bufLen, dataLen, isNull);
}

template <typename T>
inline bool MultiValueAttributeSegmentReader<T>::ReadToBuffer(docid_t docId, const ReadContextBasePtr& ctx,
                                                              uint8_t* buf, uint32_t bufLen, uint32_t& dataLen,
                                                              bool& isNull) const
{
    if (docId < 0 || docId >= (docid_t)mDocCount) {
        return false;
    }
    autil::mem_pool::Pool dummyPool;

    const uint8_t* data = ReadData(docId, *(ReadContext*)ctx.get());
    if (!data) {
        return false;
    }
    dataLen = mDataFormatter.GetDataLength(data, isNull);
    if (dataLen > bufLen) {
        return false;
    }
    memcpy(buf, data, dataLen);
    return true;
}

template <typename T>
inline bool MultiValueAttributeSegmentReader<T>::ReadDataAndLen(docid_t docId, const ReadContextBasePtr& ctx,
                                                                uint8_t* buf, uint32_t bufLen, uint32_t& dataLen)
{
    bool isNull;
    return ReadToBuffer(docId, ctx, buf, bufLen, dataLen, isNull);
}

template <typename T>
inline future_lite::coro::Lazy<std::vector<index::Result<uint8_t*>>>
MultiValueAttributeSegmentReader<T>::BatchReadData(const std::vector<docid_t>& docIds, ReadContext& ctx,
                                                   file_system::ReadOption readOption) const noexcept
{
    std::vector<index::Result<uint8_t*>> result(docIds.size(), {nullptr});
    assert(mPatchApplyStrategy != PatchApplyStrategy::PAS_APPLY_ON_READ);
    std::vector<uint64_t> offsets;
    auto offsetResult = co_await ctx.offsetReader.BatchGetOffset(docIds, readOption, &offsets);
    assert(offsetResult.size() == docIds.size());
    bool hasFailed = false;
    size_t readDataCount = 0;
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (offsetResult[i] != index::ErrorCode::OK) {
            result[i] = offsetResult[i];
            hasFailed = true;
        } else {
            size_t offset = offsets[i];
            if (!mOffsetFormatter.IsSliceArrayOffset(offset)) {
                if (!mData) {
                    readDataCount++;
                } else {
                    result[i] = mData + offset;
                }
            } else {
                assert(mDefragSliceArray);
                result[i] = (uint8_t*)mDefragSliceArray->Get(mOffsetFormatter.DecodeToSliceArrayOffset(offset));
            }
        }
    }
    if (hasFailed) {
        co_return result;
    }
    if (readDataCount) {
        assert(readDataCount == docIds.size());
        assert(ctx.pool);
        if (!ctx.pool) {
            co_return result;
        }
        std::vector<size_t> dataLengths;
        index::ErrorCodeVec dataLengthEc = co_await mDataFormatter.BatchGetDataLenghFromStream(
            ctx.fileStream, offsets, ctx.pool, readOption, &dataLengths);
        assert(dataLengthEc.size() == offsets.size());
        assert(dataLengths.size() == offsets.size());
        file_system::BatchIO dataBatchIO;
        dataBatchIO.reserve(dataLengths.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            if (dataLengthEc[i] == index::ErrorCode::OK) {
                dataBatchIO.emplace_back(IE_POOL_COMPATIBLE_NEW_VECTOR(ctx.pool, char, dataLengths[i]), dataLengths[i],
                                         offsets[i]);
            }
        }
        auto dataResult = co_await ctx.fileStream->BatchRead(dataBatchIO, readOption);
        assert(dataResult.size() == dataBatchIO.size());
        size_t bufferIdx = 0;
        for (size_t i = 0; i < offsets.size(); ++i) {
            if (dataLengthEc[i] == index::ErrorCode::OK) {
                if (dataResult[bufferIdx].OK()) {
                    result[i] = (uint8_t*)dataBatchIO[bufferIdx].buffer;
                } else {
                    result[i] = index::ConvertFSErrorCode(dataResult[bufferIdx].ec);
                }
                bufferIdx++;
            } else {
                result[i] = dataLengthEc[i];
            }
        }
    }
    co_return result;
}

template <typename T>
inline future_lite::coro::Lazy<index::ErrorCodeVec> MultiValueAttributeSegmentReader<T>::BatchRead(
    const std::vector<docid_t>& docIds, ReadContext& ctx, file_system::ReadOption readOption,
    typename std::vector<autil::MultiValueType<T>>* values, std::vector<bool>* isNullVec) const noexcept
{
    assert(values);
    assert(isNullVec);
    index::ErrorCodeVec result(docIds.size(), index::ErrorCode::OK);
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (docIds[i] < 0 || docIds[i] >= (docid_t)mDocCount) {
            co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::Runtime);
        }
    }
    values->resize(docIds.size());
    isNullVec->resize(docIds.size());
    if (mEnableAccessCountors && mAttributeMetrics && mProperty != UNKNOWN) {
        mAttributeMetrics->IncAccessCounter(mAttrConfig->GetAttrName(), mProperty, docIds.size());
    }
    std::vector<index::Result<uint8_t*>> data = co_await BatchReadData(docIds, ctx, readOption);
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (!data[i].Ok()) {
            result[i] = data[i].GetErrorCode();
            continue;
        }
        autil::MultiValueType<T> value;
        bool isNull = false;
        if (mFixedValueCount == -1) {
            value.init((const void*)data[i].Value());
            isNull = mSupportNull ? value.isNull() : false;
        } else {
            if constexpr (std::is_same<T, float>::value) {
                if (!ctx.pool) {
                    co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::Runtime);
                }
                common::FloatCompressConvertor convertor(mCompressType, mFixedValueCount);
                [[maybe_unused]] auto ret = convertor.GetValue((const char*)data[i].Value(), value, ctx.pool);
            } else {
                value = autil::MultiValueType<T>(reinterpret_cast<const char*>(data[i].Value()), mFixedValueCount);
            }
            isNull = false;
        }
        (*values)[i] = value;
        (*isNullVec)[i] = isNull;
    }
    co_return result;
}

}} // namespace indexlib::index
