#ifndef __INDEXLIB_SINGLE_ATTRIBUTE_SEGMENT_READER_FOR_MERGE_H
#define __INDEXLIB_SINGLE_ATTRIBUTE_SEGMENT_READER_FOR_MERGE_H

#include <fslib/fslib.h>
#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/numeric_compress/equivalent_compress_reader.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/misc/exception.h"

IE_NAMESPACE_BEGIN(index);

class SingleAttributeSegmentReaderForMergeBase : public AttributeSegmentReader
{
public:
    SingleAttributeSegmentReaderForMergeBase() {}
    virtual ~SingleAttributeSegmentReaderForMergeBase() {}

public:
    virtual void Open(const index::SegmentDirectoryBasePtr& segDir, 
                      const index_base::SegmentInfo& segInfo, uint32_t segmentId) = 0;
};

template <typename T>
class SingleAttributeSegmentReaderForMerge : public SingleAttributeSegmentReaderForMergeBase
{
public:
    typedef common::EquivalentCompressReader<T> EquivalentCompressReader;
    DEFINE_SHARED_PTR(EquivalentCompressReader);
public:
    SingleAttributeSegmentReaderForMerge(const config::AttributeConfigPtr& config);
    virtual ~SingleAttributeSegmentReaderForMerge();

public:
    void Open(const index::SegmentDirectoryBasePtr& segDir, 
              const index_base::SegmentInfo& segInfo, uint32_t segmentId) override;

    void Open(const index_base::PartitionDataPtr& partData, 
              const index_base::SegmentInfo& segInfo, uint32_t segmentId);

    void OpenWithoutPatch(const file_system::DirectoryPtr& attrDirectory,
                          const index_base::SegmentInfo& segInfo, uint32_t segmentId);

    bool IsInMemory() const override { return false; }
    uint32_t GetDataLength(docid_t docId) const override { return sizeof(T); }
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) override;
    uint64_t GetOffset(docid_t docId) const override { return sizeof(T) * docId; }

    static int64_t EstimateMemoryUse(bool compressData, uint32_t docCount);

protected:
    void InitPatchReader(const index_base::PartitionDataPtr& partData);
    bool Read(docid_t docId, T& value);
    bool ReadCompressData(docid_t docId, T& value);

protected:
    static const size_t BUFFER_SIZE = 1024 * 64;

    config::AttributeConfigPtr mAttrConfig;
    segmentid_t mSegmentId;

    file_system::BufferedFileReaderPtr mDataFile;
    uint8_t * mDataBuffer;
    uint32_t mDocCount;
    docid_t mStartDocId;
    int32_t mValueCountInBuffer;

    EquivalentCompressReaderPtr mCompressReader;
    SingleValueAttributePatchReader<T> mPatchReader;
    IE_LOG_DECLARE();
private:
    friend class SingleAttributeSegmentReaderForMergeTest;
};

IE_LOG_SETUP_TEMPLATE(index, SingleAttributeSegmentReaderForMerge);
//////////////////////////////////////////////////////////////////
template<typename T>
SingleAttributeSegmentReaderForMerge<T>::SingleAttributeSegmentReaderForMerge(
        const config::AttributeConfigPtr& config)
    : mAttrConfig(config)
    , mSegmentId(-1)
    , mDataBuffer(NULL)
    , mDocCount(0)
    , mStartDocId(0)
    , mValueCountInBuffer(0)
    , mPatchReader(config)
{
    mDataBuffer = (uint8_t *)malloc(BUFFER_SIZE * sizeof(T));

    if (!mDataBuffer)
    {
        INDEXLIB_FATAL_ERROR(OutOfMemory, "Allocate data buffer failed.");
    }
}

template<typename T>
SingleAttributeSegmentReaderForMerge<T>::~SingleAttributeSegmentReaderForMerge()
{
    if (mDataBuffer)
    {
        free(mDataBuffer);
        mDataBuffer = NULL;
    }
}

template<typename T>
inline void SingleAttributeSegmentReaderForMerge<T>::Open(
        const index::SegmentDirectoryBasePtr& segDir, 
        const index_base::SegmentInfo& segInfo, uint32_t segmentId)
{
    assert(segInfo.docCount > 0);

    index_base::PartitionDataPtr partitionData = segDir->GetPartitionData();
    assert(partitionData);

    Open(partitionData, segInfo, segmentId);
}

template<typename T>
inline void SingleAttributeSegmentReaderForMerge<T>::Open(
        const index_base::PartitionDataPtr& partitionData,
        const index_base::SegmentInfo& segInfo, uint32_t segmentId)
{
    index_base::SegmentData segmentData = 
        partitionData->GetSegmentData(segmentId);
    file_system::DirectoryPtr attrDirectory = 
        segmentData.GetAttributeDirectory(mAttrConfig->GetAttrName(), true);

    OpenWithoutPatch(attrDirectory, segInfo, segmentId);
    InitPatchReader(partitionData);

    IE_LOG(DEBUG, "Finishing loading segment(%d) for attribute(%s)... ", 
           (int32_t)segmentId, mAttrConfig->GetAttrName().c_str());
}

template<typename T>
inline void SingleAttributeSegmentReaderForMerge<T>::OpenWithoutPatch(
        const file_system::DirectoryPtr& attrDirectory,
        const index_base::SegmentInfo& segInfo, uint32_t segmentId)
{
    IE_LOG(DEBUG, "Start loading segment(%d) for attribute(%s)... ", 
           (int32_t)segmentId, mAttrConfig->GetAttrName().c_str());

    file_system::FileReaderPtr fileReader = 
        attrDirectory->CreateFileReader(ATTRIBUTE_DATA_FILE_NAME, 
                file_system::FSOT_BUFFERED);

    mDataFile = DYNAMIC_POINTER_CAST(
            file_system::BufferedFileReader, fileReader);
    assert(mDataFile);

    size_t fileLen = mDataFile->GetLength();
    if (AttributeCompressInfo::NeedCompressData(mAttrConfig))
    {
        free(mDataBuffer);
        mDataBuffer = (uint8_t *)malloc(fileLen);
        size_t readed = mDataFile->Read((void*)mDataBuffer, fileLen);
        assert(readed == fileLen);
        (void) readed;
        mCompressReader.reset(new EquivalentCompressReader(mDataBuffer));     
        mDocCount = mCompressReader->Size();
        mDataFile.reset();
    }
    else
    {
        mDocCount = fileLen / sizeof(T);
    }

    if (mDocCount != segInfo.docCount)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Attribute data file length is not consistent with segment info,"
                             "data length is %ld, doc number in segment info is %d", 
                             fileLen, (int32_t)segInfo.docCount);
    }

    IE_LOG(DEBUG, "Loaded segment(%d)'s data file for attribute(%s) ", 
           (int32_t)segmentId, mAttrConfig->GetAttrName().c_str());

    mSegmentId = segmentId;
}

template <typename T>
bool SingleAttributeSegmentReaderForMerge<T>::Read(docid_t docId, 
        uint8_t* buf, uint32_t bufLen)
{
    if (bufLen >= sizeof(T))
    {
        T& value = *((T*)(buf));

        if (mCompressReader)
        {
            return ReadCompressData(docId, value);
        }
        return Read(docId, value);
    }
    return false;    
}

template <typename T>
bool SingleAttributeSegmentReaderForMerge<T>::ReadCompressData(docid_t docId, T& value)
{
    assert(mCompressReader);
    if (docId >= (docid_t)mDocCount)
    {
        return false;
    }
    if (mPatchReader.ReadPatch(docId, value))
    {
        return true;
    }
    value = mCompressReader->Get(docId);
    return true;
}

template <typename T>
bool SingleAttributeSegmentReaderForMerge<T>::Read(docid_t docId, T& value)
{
    if (docId >= (docid_t)mDocCount)
    {
        return false;
    }
    if (mPatchReader.ReadPatch(docId, value))
    {
        return true;
    }
    docid_t toReadDocId = docId - mStartDocId;
    if (toReadDocId >= (docid_t)mValueCountInBuffer)
    {
        int64_t cursor = mDataFile->Tell();
        cursor += (toReadDocId - mValueCountInBuffer) * sizeof(T);

        size_t readed = mDataFile->Read((void*)mDataBuffer, 
                BUFFER_SIZE * sizeof(T), cursor);
        if (readed == 0)
        {
            return false;
        }
        mStartDocId = docId;
        mValueCountInBuffer = readed / sizeof(T);
    }
    value = *(T*)(mDataBuffer + (docId - mStartDocId)*sizeof(T));
    return true;
}

template<typename T>
inline void SingleAttributeSegmentReaderForMerge<T>::InitPatchReader(
        const index_base::PartitionDataPtr& partData)
{
    assert(partData);
    index_base::PatchFileFinderPtr patchFinder = 
        index_base::PatchFileFinderCreator::Create(partData.get());

    index_base::AttrPatchFileInfoVec patchVec;
    patchFinder->FindAttrPatchFilesForSegment(
        mAttrConfig, mSegmentId, patchVec);

    for (size_t i = 0; i < patchVec.size(); i++)
    {
        mPatchReader.AddPatchFile(patchVec[i].patchDirectory,
                patchVec[i].patchFileName, patchVec[i].srcSegment);
    }
}

template<typename T>
inline int64_t SingleAttributeSegmentReaderForMerge<T>::EstimateMemoryUse(
        bool compressData, uint32_t docCount)
{
    int64_t size = sizeof(SingleAttributeSegmentReaderForMerge<T>);
    if (!compressData)
    {
        size += BUFFER_SIZE * sizeof(T); // read buffer
    }
    else
    {
        size += docCount * sizeof(T); // the worst situation, no value equal, maybe we should use fileLen
    }
    return size;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_ATTRIBUTE_SEGMENT_READER_FOR_MERGE_H
