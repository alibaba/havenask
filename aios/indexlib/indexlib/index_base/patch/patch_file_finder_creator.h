#ifndef __INDEXLIB_PATCH_FILE_FINDER_CREATOR_H
#define __INDEXLIB_PATCH_FILE_FINDER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, PatchFileFinder);

IE_NAMESPACE_BEGIN(index_base);

class PatchFileFinderCreator
{
public:
    PatchFileFinderCreator();
    ~PatchFileFinderCreator();

public:
    static PatchFileFinderPtr Create(PartitionData* partitionData);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchFileFinderCreator);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PATCH_FILE_FINDER_CREATOR_H
