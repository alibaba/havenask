#ifndef __INDEXLIB_SEGMENT_META_H
#define __INDEXLIB_SEGMENT_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/file_system/directory.h"

// TODO: move SegmentInfo to common namespace
IE_NAMESPACE_BEGIN(table);

class SegmentMeta
{
public:
    enum class SegmentSourceType
    {
        INC_BUILD = 0,
        RT_BUILD = 1,
        UNKNOWN = -1
    };
    
public:
    SegmentMeta();
    ~SegmentMeta();
public:
    index_base::SegmentDataBase segmentDataBase;
    index_base::SegmentInfo segmentInfo;
    file_system::DirectoryPtr segmentDataDirectory;
    SegmentSourceType type;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentMeta);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_SEGMENT_META_H
