#include "indexlib/index/normal/summary/common_disk_summary_segment_reader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/buffered_file_reader.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, CommonDiskSummarySegmentReader);

CommonDiskSummarySegmentReader::CommonDiskSummarySegmentReader(
        const SummaryGroupConfigPtr& summaryGroupConfig) 
    : LocalDiskSummarySegmentReader(summaryGroupConfig)
{
}

CommonDiskSummarySegmentReader::~CommonDiskSummarySegmentReader() 
{
}

size_t CommonDiskSummarySegmentReader::GetRawDataLength(docid_t localDocId)
{
    uint32_t len;
    uint64_t offset = ReadOffset(localDocId);
    if (localDocId == (docid_t)mSegmentInfo.docCount - 1)
    {
        len = mDataFileLength - offset;
    }
    else
    {
        uint64_t nextOffset = ReadOffset(localDocId + 1);
        len = nextOffset - offset;
    }
    
    return len;
}

void CommonDiskSummarySegmentReader::GetRawData(docid_t localDocId, 
        char* rawBuf, size_t bufLen)
{
    uint64_t offset = ReadOffset(localDocId);
    mDataFileReader->Read(rawBuf, bufLen, offset);
}

file_system::FileReaderPtr CommonDiskSummarySegmentReader::LoadDataFile(
        const file_system::DirectoryPtr& directory)
{
    return directory->CreateFileReader(SUMMARY_DATA_FILE_NAME,
            file_system::FSOT_BUFFERED);
}

file_system::FileReaderPtr CommonDiskSummarySegmentReader::LoadOffsetFile(
        const file_system::DirectoryPtr& directory)
{
    return directory->CreateFileReader(SUMMARY_OFFSET_FILE_NAME,
            file_system::FSOT_IN_MEM);
}

uint64_t CommonDiskSummarySegmentReader::ReadOffset(docid_t localDocId)
{
    assert(localDocId < (docid_t)mSegmentInfo.docCount);
    return mOffsetData[localDocId];
}

int64_t CommonDiskSummarySegmentReader::EstimateMemoryUse(uint32_t docCount)
{
    return sizeof(CommonDiskSummarySegmentReader) 
        + docCount * sizeof(uint64_t) // offset
        + file_system::FSReaderParam::DEFAULT_BUFFER_SIZE; // data buffer
}

IE_NAMESPACE_END(index);

