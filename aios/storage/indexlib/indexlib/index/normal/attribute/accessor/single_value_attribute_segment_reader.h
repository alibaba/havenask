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
#include <sstream>
#include <sys/mman.h>
#include <sys/types.h>

#include "autil/TimeUtility.h"
#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressSessionReader.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/patch_apply_option.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index/normal/attribute/format/single_value_attribute_formatter.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueAttributeSegmentReader : public AttributeSegmentReader
{
public:
    typedef index::SingleValueAttributeFormatter<T> SingleValueAttributeFormatter;
    typedef index::EquivalentCompressReader<T> EquivalentCompressReader;
    DEFINE_SHARED_PTR(EquivalentCompressReader);
    struct ReadContext : public ReadContextBase {
        std::shared_ptr<file_system::FileStream> fileStream;
        EquivalentCompressSessionReader<T> compressSessionReader;
    };

public:
    SingleValueAttributeSegmentReader(const config::AttributeConfigPtr& config,
                                      AttributeMetrics* attributeMetrics = NULL,
                                      TemperatureProperty property = UNKNOWN);

    virtual ~SingleValueAttributeSegmentReader();

public:
    bool IsInMemory() const override { return false; }

    uint32_t TEST_GetDataLength(docid_t docId) const override { return mDataSize; }

    uint64_t GetOffset(docid_t docId, const ReadContextBasePtr& ctx) const override
    {
        return mFormatter.GetDataLen(docId + 1);
    }
    ReadContextBasePtr CreateReadContextPtr(autil::mem_pool::Pool* pool) const override
    {
        return ReadContextBasePtr(new ReadContext(CreateReadContext(pool)));
    }
    future_lite::coro::Lazy<ErrorCodeVec> BatchRead(const std::vector<docid_t>& docIds, ReadContext& ctx,
                                                    file_system::ReadOption readOption, typename std::vector<T>* values,
                                                    std::vector<bool>* isNullVec) const noexcept;
    bool Read(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf, uint32_t bufLen, bool& isNull) override;
    ReadContext CreateReadContext(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        ReadContext context;
        if (!mData) {
            context.fileStream = mFileStream->CreateSessionStream(sessionPool);
        }
        if (mCompressReader) {
            context.compressSessionReader =
                mCompressReader->CreateSessionReader(sessionPool, EquivalentCompressSessionOption());
        }
        return context;
    }

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull) override;
    bool Updatable() const override { return mUpdatable; }

    template <class Compare>
    void Search(T value, const DocIdRange& rangeLimit, const indexlibv2::config::SortPattern& sortType,
                docid_t& docId) const;

    int32_t SearchNullCount(const indexlibv2::config::SortPattern& sortType) const;

    // when no need to update, set @disableUpdate to true, no need prepare extend file
    void Open(const index_base::SegmentData& segData, const PatchApplyOption& patchApplyOption,
              const file_system::DirectoryPtr& attrDirectory, bool disableUpdate);

    void OpenWithoutPatch(const index_base::SegmentDataBase& segDataBase,
                          const file_system::DirectoryPtr& baseDirectory, bool supportFileCompress, bool disableUpdate);

public:
    inline bool Read(docid_t docId, T& value, bool& isNull, ReadContext& ctx) const __ALWAYS_INLINE;

    uint8_t* GetDataBaseAddr() const { return mData; }

private:
    void InitFormmater();
    virtual file_system::FileReaderPtr CreateFileReader(const file_system::DirectoryPtr& directory,
                                                        const std::string& fileName, bool supportFileCompress) const;
    file_system::FileReaderPtr CreateExtendFile(const file_system::DirectoryPtr& directory) const;

    void InitAttributeMetrics()
    {
        if (!mAttributeMetrics) {
            return;
        }
        mAttributeMetrics->IncreaseEqualCompressExpandFileLenValue(mSliceFileLen);
        mAttributeMetrics->IncreaseEqualCompressWastedBytesValue(mCompressMetrics.noUsedBytesSize);
    }

    void UpdateAttributeMetrics()
    {
        assert(mCompressReader);
        EquivalentCompressUpdateMetrics newMetrics = mCompressReader->GetUpdateMetrics();

        size_t newSliceFileLen = mSliceFileReader->GetLength();
        if (mAttributeMetrics) {
            mAttributeMetrics->IncreaseEqualCompressExpandFileLenValue(newSliceFileLen - mSliceFileLen);
            mAttributeMetrics->IncreaseEqualCompressWastedBytesValue(newMetrics.noUsedBytesSize -
                                                                     mCompressMetrics.noUsedBytesSize);
            mAttributeMetrics->IncreaseEqualCompressInplaceUpdateCountValue(newMetrics.inplaceUpdateCount -
                                                                            mCompressMetrics.inplaceUpdateCount);
            mAttributeMetrics->IncreaseEqualCompressExpandUpdateCountValue(newMetrics.expandUpdateCount -
                                                                           mCompressMetrics.expandUpdateCount);
        }
        mSliceFileLen = newSliceFileLen;
        mCompressMetrics = newMetrics;
    }

protected:
    uint32_t mDocCount;
    index::SingleValueAttributeFormatter<T> mFormatter;
    uint8_t* mData;

    PatchApplyStrategy mPatchApplyStrategy;
    EquivalentCompressReader* mCompressReader;
    file_system::SliceFileReaderPtr mSliceFileReader;
    std::shared_ptr<SingleValueAttributePatchReader<T>> mPatchReader;
    size_t mSliceFileLen;
    config::AttributeConfigPtr mAttrConfig;

    std::shared_ptr<file_system::FileStream> mFileStream;
    EquivalentCompressUpdateMetrics mCompressMetrics;
    uint8_t mDataSize;
    bool mUpdatable;

private:
    IE_LOG_DECLARE();
    friend class SingleValueAttributeSegmentReaderTest;
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeSegmentReader);

/////////////////////////////////////////////////////////
// inline functions
template <typename T>
inline SingleValueAttributeSegmentReader<T>::SingleValueAttributeSegmentReader(const config::AttributeConfigPtr& config,
                                                                               AttributeMetrics* attributeMetrics,
                                                                               TemperatureProperty property)
    : AttributeSegmentReader(attributeMetrics, property)
    , mDocCount(0)
    , mData(nullptr)
    , mPatchApplyStrategy(PatchApplyStrategy::PAS_APPLY_UNKNOW)
    , mCompressReader(nullptr)
    , mSliceFileLen(0)
    , mAttrConfig(config)
    , mUpdatable(true)
{
    if (AttributeCompressInfo::NeedCompressData(mAttrConfig)) {
        mCompressReader = new EquivalentCompressReader();
    } else {
        InitFormmater();
    }
    // ut capable
    if (mAttrConfig != nullptr) {
        const config::FieldConfigPtr fieldConfig = mAttrConfig->GetFieldConfig();
        const config::CompressTypeOption& compressType = mAttrConfig->GetCompressType();
        if (fieldConfig->GetFieldType() == FieldType::ft_float) {
            mDataSize = common::FloatCompressConvertor::GetSingleValueCompressLen(compressType);
        } else {
            mDataSize = sizeof(T);
        }
    } else {
        mDataSize = sizeof(T);
    }
}

template <typename T>
inline SingleValueAttributeSegmentReader<T>::~SingleValueAttributeSegmentReader()
{
    DELETE_AND_SET_NULL(mCompressReader);
}

template <typename T>
inline void SingleValueAttributeSegmentReader<T>::InitFormmater()
{
    if (mAttrConfig != nullptr) {
        mFormatter.Init(mAttrConfig->GetCompressType(), mAttrConfig->GetFieldConfig()->IsEnableNullField());
    } else {
        // only for test case
        mFormatter.Init();
    }
}

template <typename T>
inline void SingleValueAttributeSegmentReader<T>::Open(const index_base::SegmentData& segData,
                                                       const PatchApplyOption& patchApplyOption,
                                                       const file_system::DirectoryPtr& attrDirectory,
                                                       bool disableUpdate)
{
    autil::ScopedTime2 timer;
    IE_LOG(DEBUG, "Begin loading segment(%d) for attribute(%s)... ", segData.GetSegmentId(),
           mAttrConfig->GetAttrName().c_str());
    mPatchApplyStrategy = patchApplyOption.applyStrategy;

    file_system::DirectoryPtr baseDirectory = attrDirectory;
    if (!baseDirectory) {
        baseDirectory = segData.GetAttributeDirectory(mAttrConfig->GetAttrName(), true);
    }

    const std::shared_ptr<const index_base::SegmentInfo>& segmentInfo = segData.GetSegmentInfo();
    OpenWithoutPatch(segData, baseDirectory, SupportFileCompress(mAttrConfig, *segmentInfo), disableUpdate);

    // for kv, kkv, mDocCount is possible less than segmentInfo->docCount
    if (mDocCount > segmentInfo->docCount) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Attribute data file length is not consistent "
                             "with segment info, data length is %ld, "
                             "doc number in segment info is %d, but mDocCount is %d",
                             mFileStream->GetStreamLength(), (int32_t)segmentInfo->docCount, mDocCount);
    }

    if (mPatchApplyStrategy == PatchApplyStrategy::PAS_APPLY_ON_READ) {
        mPatchReader = DYNAMIC_POINTER_CAST(SingleValueAttributePatchReader<T>, patchApplyOption.patchReader);
    } else {
        assert(mPatchApplyStrategy == PatchApplyStrategy::PAS_APPLY_NO_PATCH);
        assert(!patchApplyOption.patchReader);
    }

    if (mCompressReader) {
        mCompressMetrics = mCompressReader->GetUpdateMetrics();
        if (mSliceFileReader) {
            mSliceFileLen = mSliceFileReader->GetLength();

            InitAttributeMetrics();
            IE_LOG(DEBUG,
                   "EquivalentCompressUpdateMetrics %s for segment(%d),"
                   "sliceFileBytes:%lu, noUseBytes:%lu, inplaceUpdate:%u, expandUpdate:%u",
                   mAttrConfig->GetAttrName().c_str(), segData.GetSegmentId(), mSliceFileLen,
                   mCompressMetrics.noUsedBytesSize, mCompressMetrics.inplaceUpdateCount,
                   mCompressMetrics.expandUpdateCount);
        }
    }
    IE_LOG(INFO, "Finishing loading segment(%d) for attribute(%s), used[%.3f]s", segData.GetSegmentId(),
           mAttrConfig->GetAttrName().c_str(), timer.done_sec());
}

template <typename T>
inline void SingleValueAttributeSegmentReader<T>::OpenWithoutPatch(const index_base::SegmentDataBase& segData,
                                                                   const file_system::DirectoryPtr& baseDirectory,
                                                                   bool supportFileCompress, bool disableUpdate)
{
    file_system::FileReaderPtr dataFile =
        CreateFileReader(baseDirectory, ATTRIBUTE_DATA_FILE_NAME, supportFileCompress);
    mFileStream = file_system::FileStreamCreator::CreateConcurrencyFileStream(dataFile, nullptr);
    mData = (uint8_t*)dataFile->GetBaseAddress();
    mUpdatable = mData != nullptr && !disableUpdate;
    if (mCompressReader) {
        if (mUpdatable) {
            auto fileReader = CreateExtendFile(baseDirectory);
            mSliceFileReader = DYNAMIC_POINTER_CAST(file_system::SliceFileReader, fileReader);
            assert(mSliceFileReader);
            mCompressReader->Init(mData, mFileStream->GetStreamLength(), mSliceFileReader->GetBytesAlignedSliceArray());
            IE_LOG(INFO, "attribute [%s] is updatable in segment [%d]", mAttrConfig->GetAttrName().c_str(),
                   segData.GetSegmentId());
        } else if (mData) {
            IE_LOG(INFO, "attribute [%s] is update-disable in segment [%d]", mAttrConfig->GetAttrName().c_str(),
                   segData.GetSegmentId());
            mCompressReader->Init(mData, mFileStream->GetStreamLength(), nullptr);
        } else {
            IE_LOG(INFO, "attribute [%s] is not updatable in segment [%d]", mAttrConfig->GetAttrName().c_str(),
                   segData.GetSegmentId());
            mCompressReader->Init(dataFile);
        }
        mDocCount = mCompressReader->Size();
    } else {
        mDocCount = mFormatter.GetDocCount(mFileStream->GetStreamLength());
    }
}

template <typename T>
inline file_system::FileReaderPtr
SingleValueAttributeSegmentReader<T>::CreateFileReader(const file_system::DirectoryPtr& directory,
                                                       const std::string& fileName, bool supportFileCompress) const
{
    file_system::ReaderOption option = file_system::ReaderOption::Writable(file_system::FSOT_LOAD_CONFIG);
    option.supportCompress = supportFileCompress;
    return directory->CreateFileReader(fileName, option);
}

template <typename T>
inline file_system::FileReaderPtr
SingleValueAttributeSegmentReader<T>::CreateExtendFile(const file_system::DirectoryPtr& directory) const
{
    std::string extendFileName = std::string(ATTRIBUTE_DATA_FILE_NAME) + ATTRIBUTE_EQUAL_COMPRESS_UPDATE_EXTEND_SUFFIX;
    file_system::FileReaderPtr fileReader = directory->CreateFileReader(extendFileName, file_system::FSOT_SLICE);
    if (!fileReader) {
        auto fileWriter = directory->CreateFileWriter(
            extendFileName,
            file_system::WriterOption::Slice(EQUAL_COMPRESS_UPDATE_SLICE_LEN, EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT));
        fileReader = directory->CreateFileReader(extendFileName, file_system::FSOT_SLICE);
        fileWriter->Close().GetOrThrow();
    }
    return fileReader;
}

template <typename T>
inline bool SingleValueAttributeSegmentReader<T>::Read(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf,
                                                       uint32_t bufLen, bool& isNull)
{
    ReadContext* readCtx = (ReadContext*)ctx.get();
    if (bufLen >= sizeof(T)) {
        T& value = *((T*)(buf));
        return Read(docId, value, isNull, *readCtx);
    }
    return false;
}

template <typename T>
inline future_lite::coro::Lazy<ErrorCodeVec>
SingleValueAttributeSegmentReader<T>::BatchRead(const std::vector<docid_t>& docIds, ReadContext& ctx,
                                                file_system::ReadOption readOption, typename std::vector<T>* values,
                                                std::vector<bool>* isNullVec) const noexcept
{
    ErrorCodeVec result(docIds.size(), ErrorCode::OK);
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (docIds[i] < 0 || docIds[i] >= (docid_t)mDocCount) {
            co_return ErrorCodeVec(docIds.size(), ErrorCode::Runtime);
        }
    }
    if (mEnableAccessCountors && mAttributeMetrics && mProperty != UNKNOWN) {
        mAttributeMetrics->IncAccessCounter(mAttrConfig->GetAttrName(), mProperty, docIds.size());
    }
    if (mPatchApplyStrategy == PatchApplyStrategy::PAS_APPLY_ON_READ && mPatchReader) {
        assert(false);
    }
    if (mCompressReader) {
        auto retVec = co_await ctx.compressSessionReader.BatchGet(docIds, readOption, values);
        isNullVec->assign(docIds.size(), false);
        for (size_t i = 0; i < retVec.size(); ++i) {
            if (ErrorCode::OK != retVec[i]) {
                result[i] = retVec[i];
            }
        }
        co_return result;
    }
    values->resize(docIds.size());
    isNullVec->resize(docIds.size());
    if (mData) {
        for (size_t i = 0; i < docIds.size(); ++i) {
            bool isNull;
            mFormatter.Get(docIds[i], mData, (*values)[i], isNull);
            (*isNullVec)[i] = isNull;
        }
        co_return result;
    }
    co_return co_await mFormatter.BatchGetFromStream(docIds, ctx.fileStream, readOption, values, isNullVec);
}

template <typename T>
inline bool SingleValueAttributeSegmentReader<T>::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull)
{
    assert(mUpdatable);
    if (docId < 0 || docId >= (docid_t)mDocCount) {
        return false;
    }
    if (!mData) {
        return true;
    }
    if (mCompressReader) {
        assert(bufLen == sizeof(T));
        bool ret = mCompressReader->Update(docId, *(T*)buf);
        UpdateAttributeMetrics();
        return ret;
    }
    assert((isNull && bufLen == 0) || bufLen == mFormatter.GetRecordSize());
    mFormatter.Set(docId, mData, *(T*)buf, isNull);
    return true;
}

template <typename T>
inline bool SingleValueAttributeSegmentReader<T>::Read(docid_t docId, T& value, bool& isNull, ReadContext& ctx) const
{
    if (docId < 0 || docId >= (docid_t)mDocCount) {
        return false;
    }
    if (mEnableAccessCountors && mAttributeMetrics && mProperty != UNKNOWN) {
        mAttributeMetrics->IncAccessCounter(mAttrConfig->GetAttrName(), mProperty, 1);
    }
    if (mPatchApplyStrategy == PatchApplyStrategy::PAS_APPLY_ON_READ && mPatchReader) {
        if (mPatchReader->ReadPatch(docId, value, isNull)) {
            return true;
        }
    }
    if (mCompressReader) {
        Status status;
        std::tie(status, value) = ctx.compressSessionReader.Get(docId);
        indexlib::util::ThrowIfStatusError(status);
        isNull = false;
        return true;
    }
    if (mData) {
        mFormatter.Get(docId, mData, value, isNull);
    } else {
        mFormatter.GetFromStream(docId, ctx.fileStream, value, isNull);
    }
    return true;
}

template <typename T>
template <class Compare>
void SingleValueAttributeSegmentReader<T>::Search(T value, const DocIdRange& rangeLimit,
                                                  const indexlibv2::config::SortPattern& sortType, docid_t& docId) const
{
    assert(mPatchApplyStrategy == PatchApplyStrategy::PAS_APPLY_NO_PATCH);
    docid_t first = rangeLimit.first;
    docid_t end = rangeLimit.second;
    docid_t count = end - first;
    docid_t step = 0;
    docid_t cur = 0;
    bool isNull = false;
    Status status;
    while (count > 0) {
        cur = first;
        step = count / 2;
        cur += step;
        T curValue = 0;
        if (mCompressReader) {
            std::tie(status, curValue) = mCompressReader->Get(cur);
            indexlib::util::ThrowIfStatusError(status);
        } else {
            if (mData) {
                mFormatter.Get(cur, mData, curValue, isNull);
            } else {
                mFormatter.GetFromStream(cur, mFileStream, curValue, isNull);
            }
        }
        if (isNull) {
            if (sortType == indexlibv2::config::sp_asc) {
                // move pointer to right as NULL value is always in the left part
                first = ++cur;
                count -= step + 1;
            } else if (sortType == indexlibv2::config::sp_desc) {
                // move to left as NULL value is always in the right part
                count = step;
            }
        } else {
            if (Compare()(value, curValue)) {
                // move to right
                first = ++cur;
                count -= step + 1;
            } else {
                // move to left
                count = step;
            }
        }
    }
    docId = first;
}

template <typename T>
int32_t SingleValueAttributeSegmentReader<T>::SearchNullCount(const indexlibv2::config::SortPattern& sortType) const
{
    assert(mPatchApplyStrategy == PatchApplyStrategy::PAS_APPLY_NO_PATCH);
    if (mCompressReader || !mFormatter.IsSupportNull()) {
        return 0;
    }
    DocIdRange rangeLimit(0, mDocCount);
    T value = 0;
    T encodedValue = mFormatter.GetEncodedNullValue();
    docid_t docId = INVALID_DOCID;
    if (sortType == indexlibv2::config::sp_asc) {
        Search<std::greater_equal<T>>(encodedValue, rangeLimit, sortType, docId);
        while (docId >= 0) {
            bool isNull = false;
            if (mData) {
                mFormatter.Get(docId, mData, value, isNull);
            } else {
                mFormatter.GetFromStream(docId, mFileStream, value, isNull);
            }
            if (isNull) {
                return docId + 1;
            }
            docId--;
        }
    } else if (sortType == indexlibv2::config::sp_desc) {
        Search<std::less_equal<T>>(encodedValue, rangeLimit, sortType, docId);
        while (docId < (docid_t)mDocCount) {
            bool isNull = false;
            if (mData) {
                mFormatter.Get(docId, mData, value, isNull);
            } else {
                mFormatter.GetFromStream(docId, mFileStream, value, isNull);
            }
            if (isNull) {
                return mDocCount - docId;
            }
            docId++;
        }
    } else {
        assert(false);
    }
    return 0;
}
}} // namespace indexlib::index
