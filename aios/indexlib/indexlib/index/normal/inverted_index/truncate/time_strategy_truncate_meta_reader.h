#ifndef __INDEXLIB_TIME_STRATEGY_TRUNCATE_META_READER_H
#define __INDEXLIB_TIME_STRATEGY_TRUNCATE_META_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_meta_reader.h"

IE_NAMESPACE_BEGIN(index);

class TimeStrategyTruncateMetaReader : public TruncateMetaReader
{
public:
    TimeStrategyTruncateMetaReader(int64_t minTime, int64_t maxTime, 
                                   bool desc = false);
    ~TimeStrategyTruncateMetaReader();

public:
    
    virtual bool Lookup(dictkey_t key, int64_t &min, int64_t &max) override;

private:
    int64_t mMinTime;
    int64_t mMaxTime;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TimeStrategyTruncateMetaReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TIME_STRATEGY_TRUNCATE_META_READER_H
