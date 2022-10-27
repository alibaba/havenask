#ifndef __INDEXLIB_PARTITION_MERGER_H
#define __INDEXLIB_PARTITION_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"

IE_NAMESPACE_BEGIN(merger);

class PartitionMerger
{
public:
    PartitionMerger();
    virtual ~PartitionMerger();

public:
    virtual bool Merge(bool optimize = false, int64_t currentTs = 0) = 0;
    virtual index_base::Version GetMergedVersion() const = 0;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionMerger);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_PARTITION_MERGER_H
