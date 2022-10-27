#ifndef __INDEXLIB_COMMON_DISK_SUMMARY_SEGMENT_READER_H
#define __INDEXLIB_COMMON_DISK_SUMMARY_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/local_disk_summary_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class CommonDiskSummarySegmentReader : public LocalDiskSummarySegmentReader
{
public:
    CommonDiskSummarySegmentReader(const config::SummaryGroupConfigPtr& summaryGroupConfig);
    virtual ~CommonDiskSummarySegmentReader();

public:
    size_t GetRawDataLength(docid_t localDocId) override;
    void GetRawData(docid_t localDocId, char* rawBuf, size_t bufLen) override;

public:
    static int64_t EstimateMemoryUse(uint32_t docCount);

protected:
    file_system::FileReaderPtr LoadDataFile(
            const file_system::DirectoryPtr& directory) override;
    file_system::FileReaderPtr LoadOffsetFile(
            const file_system::DirectoryPtr& directory) override;

public:
    uint64_t ReadOffset(docid_t localDocId);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CommonDiskSummarySegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_COMMON_DISK_SUMMARY_SEGMENT_READER_H
