#include <sys/mman.h>
#include "indexlib/index/normal/summary/local_disk_summary_segment_reader.h"
#include "indexlib/util/mmap_allocator.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, LocalDiskSummarySegmentReader);

LocalDiskSummarySegmentReader::LocalDiskSummarySegmentReader(
        const SummaryGroupConfigPtr& summaryGroupConfig) 
    : mOffsetData(NULL)
    , mSummaryGroupConfig(summaryGroupConfig)
    , mOffsetFileLength(0)
    , mDataFileLength(0)
{
}

LocalDiskSummarySegmentReader::~LocalDiskSummarySegmentReader() 
{
}

bool LocalDiskSummarySegmentReader::Open(const index_base::SegmentData& segmentData)
{
    mSegmentInfo = segmentData.GetSegmentInfo();

    DirectoryPtr summaryDirectory = segmentData.GetSummaryDirectory(true);
    DirectoryPtr directory = summaryDirectory;
    if (!mSummaryGroupConfig->IsDefaultGroup())
    {
        const string& groupName = mSummaryGroupConfig->GetGroupName();
        directory = summaryDirectory->GetDirectory(groupName, true);
    }

    mOffsetFileReader = LoadOffsetFile(directory);
    mOffsetData = (uint64_t*)mOffsetFileReader->GetBaseAddress();
    mOffsetFileLength = mOffsetFileReader->GetLength();

    mDataFileReader = LoadDataFile(directory);
    mDataFileLength = mDataFileReader->GetLength();

    uint32_t docCountInOffsetFile = mOffsetFileLength / sizeof(uint64_t);
    if (mSegmentInfo.docCount != docCountInOffsetFile)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Doc count(%d : %d) in segment info"
                             " and offset file doesn't match: %s", 
                             (int32_t)mSegmentInfo.docCount,
                             (int32_t)docCountInOffsetFile, 
                             summaryDirectory->GetPath().c_str());
    }
    return true;
}

file_system::FileReaderPtr LocalDiskSummarySegmentReader::LoadDataFile(
        const file_system::DirectoryPtr& directory)
{
    return directory->CreateFileReader(SUMMARY_DATA_FILE_NAME,
            file_system::FSOT_LOAD_CONFIG);
}

file_system::FileReaderPtr LocalDiskSummarySegmentReader::LoadOffsetFile(
        const file_system::DirectoryPtr& directory)
{
    return directory->CreateIntegratedFileReader(SUMMARY_OFFSET_FILE_NAME);
}

bool LocalDiskSummarySegmentReader::GetDocument(docid_t localDocId, 
        SearchSummaryDocument *summaryDoc) const
{
    assert(localDocId != INVALID_DOCID);
    assert(mOffsetData);

    uint64_t offset = mOffsetData[localDocId];
    uint32_t len;
    if (localDocId == (docid_t)mSegmentInfo.docCount - 1)
    {
        len = mDataFileLength - offset;
    }
    else
    {
        len = mOffsetData[localDocId + 1] - offset;
    }

    if (len == 0)
    {
        return false; 
    }
    
    //TODO: var length memory allocate and copy now
    char defValue[8192];
    char* value = len < sizeof(defValue) ? defValue : new char[len];
    mDataFileReader->Read((void*)value, len, offset);

    SummaryGroupFormatter formatter(mSummaryGroupConfig);
    if (formatter.DeserializeSummary(summaryDoc, value, len))
    {
        if (value != defValue)
        {
            delete []value;
        }
        return true;
    }
    else
    {
        if (value != defValue)
        {
            delete []value;
        }
        stringstream ss;
        ss << "Deserialize summary[docid = " << localDocId << "] FAILED.";
        INDEXLIB_THROW(misc::IndexCollapsedException, "%s", ss.str().c_str());
        return false;
    }
    return false;
}

IE_NAMESPACE_END(index);

