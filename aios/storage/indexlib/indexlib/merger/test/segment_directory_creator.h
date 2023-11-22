#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/segment_directory.h"

namespace indexlib { namespace merger {

class SegmentDirectoryCreator
{
public:
    SegmentDirectoryCreator();
    ~SegmentDirectoryCreator();

public:
    // data format: (version_id:timestamp:segid,segid,...)
    // we use segid as doc count by default
    static SegmentDirectoryPtr CreateSegmentDirectory(const std::string& dataStr,
                                                      const indexlib::file_system::DirectoryPtr& rootDir);

    // data format: (version_id:timestamp:segid, segid,...)
    // we use segid as doc count by default
    static index_base::Version MakeVersion(const std::string& dataStr, const file_system::DirectoryPtr& rootDir,
                                           bool hasSub = false);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDirectoryCreator);
}} // namespace indexlib::merger
