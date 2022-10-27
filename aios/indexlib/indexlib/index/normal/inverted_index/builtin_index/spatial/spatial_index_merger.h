#ifndef __INDEXLIB_SPATIAL_INDEX_MERGER_H
#define __INDEXLIB_SPATIAL_INDEX_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class SpatialIndexMerger : public TextIndexMerger
{
public:
    DECLARE_INDEX_MERGER_IDENTIFIER(spatial);
    DECLARE_INDEX_MERGER_CREATOR(SpatialIndexMerger, it_spatial);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialIndexMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SPATIAL_INDEX_MERGER_H
