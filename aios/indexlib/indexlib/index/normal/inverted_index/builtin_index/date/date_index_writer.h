#ifndef __INDEXLIB_DATE_INDEX_WRITER_H
#define __INDEXLIB_DATE_INDEX_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/config/date_level_format.h"

IE_NAMESPACE_BEGIN(index);

class TimeRangeInfo;
DEFINE_SHARED_PTR(TimeRangeInfo);

class DateIndexWriter : public index::NormalIndexWriter
{
public:
    DateIndexWriter(size_t lastSegmentDistinctTermCount,
                    const config::IndexPartitionOptions& options);
    ~DateIndexWriter();
public:
    void Init(const config::IndexConfigPtr& indexConfig,
              util::BuildResourceMetrics* buildResourceMetrics,
              const index_base::PartitionSegmentIteratorPtr& segIter =
              index_base::PartitionSegmentIteratorPtr()) override;
    
    void AddField(const document::Field* field) override;
    void Dump(const file_system::DirectoryPtr& dir,
              autil::mem_pool::PoolBase* dumpPool) override;
    index::IndexSegmentReaderPtr CreateInMemReader() override;
private:
    config::DateLevelFormat mFormat;
    TimeRangeInfoPtr mTimeRangeInfo;
    uint64_t mGranularityLevel;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DATE_INDEX_WRITER_H
