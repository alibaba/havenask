#ifndef __INDEXLIB_PATCH_ITERATOR_CREATOR_H
#define __INDEXLIB_PATCH_ITERATOR_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index, PatchIterator);

IE_NAMESPACE_BEGIN(index);

class PatchIteratorCreator
{
public:
    PatchIteratorCreator();
    ~PatchIteratorCreator();
public:
    static PatchIteratorPtr Create(const config::IndexPartitionSchemaPtr& schema,
                                   const index_base::PartitionDataPtr& partitionData,
                                   bool isIncConsistentWithRt,
                                   const index_base::Version& lastLoadVersion,
                                   segmentid_t lastLoadedSegment);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchIteratorCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PATCH_ITERATOR_CREATOR_H
