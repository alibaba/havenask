#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_SEGMENT_READER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_SEGMENT_READER_H

#include <tr1/memory>
#include <sstream>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "fslib/fslib.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/common/numeric_compress/equivalent_compress_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/format/single_value_attribute_formatter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class SingleValueAttributeSegmentReader : public AttributeSegmentReader
{
public:
    typedef index::SingleValueAttributeFormatter<T> SingleValueAttributeFormatter;
    typedef common::EquivalentCompressReader<T> EquivalentCompressReader;
    DEFINE_SHARED_PTR(EquivalentCompressReader);

public:
    SingleValueAttributeSegmentReader(
            const config::AttributeConfigPtr& config,
            AttributeMetrics* attributeMetrics = NULL);

    virtual ~SingleValueAttributeSegmentReader();

public:
    bool IsInMemory() const override { return false; }

    uint32_t GetDataLength(docid_t docId) const override
    {
        return sizeof(T);
    }

    uint64_t GetOffset(docid_t docId) const override
    {
        return mDataSize * docId;
    }

    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) override;

    bool UpdateField(docid_t docId, uint8_t* buf,
                                    uint32_t bufLen) override;

    template<class Compare>
    void Search(T value, const DocIdRange& rangeLimit, docid_t& docId) const;

    void Open(const index_base::SegmentData& segData,
              const file_system::DirectoryPtr& attrDirectory =
              file_system::DirectoryPtr(),
              const AttributeSegmentPatchIteratorPtr& segmentPatchIterator =
              AttributeSegmentPatchIteratorPtr());

    void OpenWithoutPatch(const index_base::SegmentDataBase& segDataBase,
                          const file_system::DirectoryPtr& baseDirectory);

public:
    inline bool Read(docid_t docId, T& value,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;

    const file_system::FileReaderPtr& GetDataFile() const { return mDataFile; }

    uint8_t* GetDataBaseAddr() const { return mData; }

private:
    void InitFormmater();
    virtual file_system::FileReaderPtr CreateFileReader(
            const file_system::DirectoryPtr& directory,
            const std::string& fileName) const;

    void InitAttributeMetrics()
    {
        if (!mAttributeMetrics)
        {
            return;
        }
        mAttributeMetrics->IncreaseEqualCompressExpandFileLenValue(
                mSliceFileLen);
        mAttributeMetrics->IncreaseEqualCompressWastedBytesValue(
                mCompressMetrics.noUsedBytesSize);
    }

    void UpdateAttributeMetrics()
    {
        assert(mCompressReader);
        common::EquivalentCompressUpdateMetrics newMetrics =
            mCompressReader->GetUpdateMetrics();

        size_t newSliceFileLen = mSliceFileReader->GetLength();
        if (mAttributeMetrics)
        {
            mAttributeMetrics->IncreaseEqualCompressExpandFileLenValue(
                    newSliceFileLen - mSliceFileLen);
            mAttributeMetrics->IncreaseEqualCompressWastedBytesValue(
                    newMetrics.noUsedBytesSize - mCompressMetrics.noUsedBytesSize);
            mAttributeMetrics->IncreaseEqualCompressInplaceUpdateCountValue(
                    newMetrics.inplaceUpdateCount - mCompressMetrics.inplaceUpdateCount);
            mAttributeMetrics->IncreaseEqualCompressExpandUpdateCountValue(
                    newMetrics.expandUpdateCount - mCompressMetrics.expandUpdateCount);
        }
        mSliceFileLen = newSliceFileLen;
        mCompressMetrics = newMetrics;
    }

protected:
    uint32_t mDocCount;
    EquivalentCompressReader* mCompressReader;
    SingleValueAttributeFormatter mFormatter;
    uint8_t* mData;

    file_system::FileReaderPtr mDataFile;
    index_base::SegmentInfo mSegmentInfo;
    file_system::SliceFileReaderPtr mSliceFileReader;
    size_t mSliceFileLen;
    config::AttributeConfigPtr mAttrConfig;

    common::EquivalentCompressUpdateMetrics mCompressMetrics;
    uint8_t mDataSize;

private:
    IE_LOG_DECLARE();
    friend class SingleValueAttributeSegmentReaderTest;
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeSegmentReader);

/////////////////////////////////////////////////////////
//inline functions
template<typename T>
inline SingleValueAttributeSegmentReader<T>::SingleValueAttributeSegmentReader(
        const config::AttributeConfigPtr& config,
        AttributeMetrics* attributeMetrics)
    : AttributeSegmentReader(attributeMetrics)
    , mDocCount(0)
    , mCompressReader(NULL)
    , mData(NULL)
    , mSliceFileLen(0)
    , mAttrConfig(config)
{
    if (AttributeCompressInfo::NeedCompressData(mAttrConfig))
    {
        mCompressReader = new EquivalentCompressReader();
    }
    else
    {
        InitFormmater();
    }
    // ut capable
    if (mAttrConfig != nullptr)
    {
        const config::FieldConfigPtr fieldConfig = mAttrConfig->GetFieldConfig();
        const config::CompressTypeOption &compressType = fieldConfig->GetCompressType();
        if (fieldConfig->GetFieldType() == FieldType::ft_float)
        {
            mDataSize = common::FloatCompressConvertor::GetSingleValueCompressLen(compressType);
        }
        else
        {
            mDataSize = sizeof(T);
        }
    }
    else
    {
        mDataSize = sizeof(T);
    }
}

template<typename T>
inline SingleValueAttributeSegmentReader<T>::~SingleValueAttributeSegmentReader()
{
    DELETE_AND_SET_NULL(mCompressReader);
}

template<typename T>
inline void SingleValueAttributeSegmentReader<T>::InitFormmater()
{
    if (mAttrConfig != nullptr) {
        mFormatter.Init(0, sizeof(T), mAttrConfig->GetFieldConfig()->GetCompressType());
    } else {
        // only for test case
        mFormatter.Init(0, sizeof(T));
    }
}

template<typename T>
inline void SingleValueAttributeSegmentReader<T>::Open(
        const index_base::SegmentData& segData,
        const file_system::DirectoryPtr& attrDirectory,
        const AttributeSegmentPatchIteratorPtr& segmentPatchIterator)
{
    IE_LOG(INFO, "Begin loading segment(%d) for attribute(%s)... ",
           segData.GetSegmentId(), mAttrConfig->GetAttrName().c_str());

    file_system::DirectoryPtr baseDirectory = attrDirectory;
    if (!baseDirectory)
    {
        baseDirectory = segData.GetAttributeDirectory(
                mAttrConfig->GetAttrName(), true);
    }

    mSegmentInfo = segData.GetSegmentInfo();
    OpenWithoutPatch(segData, baseDirectory);

    if (mDocCount != mSegmentInfo.docCount)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Attribute data file length is not consistent "
                             "with segment info, data length is %ld, "
                             "doc number in segment info is %d",
                             mDataFile->GetLength(),
                             (int32_t)mSegmentInfo.docCount);
    }

    if (segmentPatchIterator)
    {
        LoadPatches(segmentPatchIterator);
    }

    if (mCompressReader)
    {
        mCompressMetrics = mCompressReader->GetUpdateMetrics();
        mSliceFileLen = mSliceFileReader->GetLength();

        InitAttributeMetrics();

        IE_LOG(DEBUG, "EquivalentCompressUpdateMetrics %s for segment(%d),"
               "sliceFileBytes:%lu, noUseBytes:%lu, inplaceUpdate:%u, expandUpdate:%u",
               mAttrConfig->GetAttrName().c_str(), segData.GetSegmentId(),
               mSliceFileLen, mCompressMetrics.noUsedBytesSize,
               mCompressMetrics.inplaceUpdateCount,
               mCompressMetrics.expandUpdateCount);
    }
    IE_LOG(INFO, "Finishing loading segment(%d) for attribute(%s)... ",
           segData.GetSegmentId(), mAttrConfig->GetAttrName().c_str());
}

template<typename T>
inline void SingleValueAttributeSegmentReader<T>::OpenWithoutPatch(
        const index_base::SegmentDataBase& segData,
        const file_system::DirectoryPtr& baseDirectory)
{
    mDataFile = CreateFileReader(baseDirectory, ATTRIBUTE_DATA_FILE_NAME);
    mData = (uint8_t* )mDataFile->GetBaseAddress();
    if (mCompressReader)
    {
        std::string extendFileName = std::string(ATTRIBUTE_DATA_FILE_NAME)
                                     + EQUAL_COMPRESS_UPDATE_EXTEND_SUFFIX;

        file_system::SliceFilePtr sliceFile;
        if (baseDirectory->IsExist(extendFileName))
        {
            sliceFile = baseDirectory->GetSliceFile(extendFileName);
        }
        else
        {
            sliceFile = baseDirectory->CreateSliceFile(
                    extendFileName, EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                    EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT);
        }

        mSliceFileReader = sliceFile->CreateSliceFileReader();
        mCompressReader->Init(mData, mDataFile->GetLength(),
                              mSliceFileReader->GetBytesAlignedSliceArray());

        mDocCount = mCompressReader->Size();
    }
    else
    {
        mDocCount = mDataFile->GetLength() / mFormatter.GetRecordSize();
    }
}

template<typename T>
inline file_system::FileReaderPtr
SingleValueAttributeSegmentReader<T>::CreateFileReader(
        const file_system::DirectoryPtr& directory,
        const std::string& fileName) const
{
    return directory->CreateWritableFileReader(fileName, file_system::FSOT_MMAP);
}

template<typename T>
inline bool SingleValueAttributeSegmentReader<T>::Read(docid_t docId,
        uint8_t* buf, uint32_t bufLen)
{
    if (bufLen >= sizeof(T))
    {
        T& value = *((T*)(buf));
        return Read(docId, value);
    }
    return false;
}

template<typename T>
inline bool SingleValueAttributeSegmentReader<T>::UpdateField(docid_t docId,
        uint8_t *buf, uint32_t bufLen)
{
    if (docId < 0 || docId >= (docid_t)mDocCount)
    {
        return false;
    }

    if (mCompressReader)
    {
        assert(bufLen == sizeof(T));
        bool ret = mCompressReader->Update(docId, *(T*)buf);
        UpdateAttributeMetrics();
        return ret;
    }
    assert(bufLen == mFormatter.GetRecordSize());
    mFormatter.Set(docId, mData, *(T *)buf);
    return true;
}

template<typename T>
inline bool SingleValueAttributeSegmentReader<T>::Read(
        docid_t docId, T& value, autil::mem_pool::Pool* pool) const
{
    if (docId < 0 || docId >= (docid_t)mDocCount)
    {
        return false;
    }
    if (mCompressReader)
    {
        value = mCompressReader->Get(docId);
    }
    else
    {
        mFormatter.Get(docId, mData, value);
    }
    return true;
}

template<typename T>
template<class Compare>
void SingleValueAttributeSegmentReader<T>::Search(
        T value, const DocIdRange& rangeLimit, docid_t& docId) const
{
    docid_t first = rangeLimit.first;
    docid_t end = rangeLimit.second;
    docid_t count = end - first;
    docid_t step = 0;
    docid_t cur = 0;
    while (count > 0) {
        cur = first;
        step = count / 2;
        cur += step;
        T curValue = 0;
        if (mCompressReader)
        {
            curValue = mCompressReader->Get(cur);
        }
        else
        {
            mFormatter.Get(cur, mData, curValue);
        }

        if (Compare()(value, curValue)) {
            first = ++cur;
            count -= step + 1;
        } else {
            count = step;
        }
    }
    docId = first;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_SEGMENT_READER_H
