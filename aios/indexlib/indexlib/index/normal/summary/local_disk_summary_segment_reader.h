#ifndef __INDEXLIB_LOCAL_DISK_SUMMARY_SEGMENT_READER_H
#define __INDEXLIB_LOCAL_DISK_SUMMARY_SEGMENT_READER_H

#include <tr1/memory>
#include "fslib/fslib.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/index/normal/summary/summary_segment_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/document/index_document/normal_document/summary_group_formatter.h"

IE_NAMESPACE_BEGIN(index);

class LocalDiskSummarySegmentReader : public SummarySegmentReader
{
public:
    LocalDiskSummarySegmentReader(const config::SummaryGroupConfigPtr& summaryGroupConfig);
    ~LocalDiskSummarySegmentReader();

public:
    bool Open(const index_base::SegmentData& segmentData);

    bool GetDocument(
            docid_t localDocId, document::SearchSummaryDocument *summaryDoc) const override;

    size_t GetRawDataLength(docid_t localDocId) override 
    { assert(false); return 0; }

    void GetRawData(docid_t localDocId, char* rawData, size_t len) override
    { assert(false); }

protected:
    virtual file_system::FileReaderPtr LoadDataFile(
            const file_system::DirectoryPtr& directory);

    virtual file_system::FileReaderPtr LoadOffsetFile(
            const file_system::DirectoryPtr& directory);

protected:
    uint64_t* mOffsetData;
    file_system::FileReaderPtr mDataFileReader;
    file_system::FileReaderPtr mOffsetFileReader;
    config::SummaryGroupConfigPtr mSummaryGroupConfig;
    
    uint64_t mOffsetFileLength;
    uint64_t mDataFileLength;
    index_base::SegmentInfo mSegmentInfo;

private:
    friend class LocalDiskSummarySegmentReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocalDiskSummarySegmentReader);

////////////////////////////////////////////

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LOCAL_DISK_SUMMARY_SEGMENT_READER_H
