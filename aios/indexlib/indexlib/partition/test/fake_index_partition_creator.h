#ifndef __INDEXLIB_FAKE_INDEX_PARTITION_CREATOR_H
#define __INDEXLIB_FAKE_INDEX_PARTITION_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition.h"

IE_NAMESPACE_BEGIN(partition);

class FakeIndexPartitionCreator
{
public:
    FakeIndexPartitionCreator();
    ~FakeIndexPartitionCreator();
public:
    static IndexPartitionPtr Create(const config::IndexPartitionSchemaPtr &schema,
                                    const IndexPartitionReaderPtr &reader);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeIndexPartitionCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FAKE_INDEX_PARTITION_CREATOR_H
