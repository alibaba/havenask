#ifndef __INDEXLIB_SEGMENT_TOPOLOGY_INFO_H
#define __INDEXLIB_SEGMENT_TOPOLOGY_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/legacy/jsonizable.h>
#include "indexlib/index_base/index_meta/level_info.h"

IE_NAMESPACE_BEGIN(index_base);

struct SegmentTopologyInfo : autil::legacy::Jsonizable
{
public:
    SegmentTopologyInfo();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    LevelTopology levelTopology;
    uint32_t levelIdx;
    bool isBottomLevel;
    size_t columnCount;// TODO remove?
    size_t columnIdx;
};

DEFINE_SHARED_PTR(SegmentTopologyInfo);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_TOPOLOGY_INFO_H
