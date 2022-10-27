#ifndef __INDEXLIB_SEGMENT_DIRECTORY_CREATOR_H
#define __INDEXLIB_SEGMENT_DIRECTORY_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/segment_directory.h"

IE_NAMESPACE_BEGIN(merger);

class SegmentDirectoryCreator
{
public:
    SegmentDirectoryCreator();
    ~SegmentDirectoryCreator();
public:
    // data format: (version_id:timestamp:segid,segid,...)
    // we use segid as doc count by default 
    static SegmentDirectoryPtr CreateSegmentDirectory(const std::string& dataStr, 
            const std::string& rootDir);

    // data format: (version_id:timestamp:segid, segid,...)
    // we use segid as doc count by default 
    static index_base::Version MakeVersion(const std::string& dataStr, 
                                               const std::string& rootDir,
                                               bool hasSub = false);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDirectoryCreator);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_SEGMENT_DIRECTORY_CREATOR_H
