#ifndef __INDEXLIB_SEGMENT_DIRECTORY_CREATOR_H
#define __INDEXLIB_SEGMENT_DIRECTORY_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);
DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);

IE_NAMESPACE_BEGIN(file_system);

typedef std::vector<DirectoryPtr> DirectoryVector;

IE_NAMESPACE_END(file_system);

IE_NAMESPACE_BEGIN(index_base);

class SegmentDirectoryCreator
{
public:
    SegmentDirectoryCreator();
    ~SegmentDirectoryCreator();

public:
    static SegmentDirectoryPtr Create(const config::IndexPartitionOptions& options,
            const file_system::DirectoryPtr& directory,
            const index_base::Version& version = INVALID_VERSION,
            bool hasSub = false);

    static SegmentDirectoryPtr Create(
            const file_system::IndexlibFileSystemPtr& fileSystem,
            index_base::Version version = INVALID_VERSION,
            const std::string& dir = "", 
            const config::IndexPartitionOptions* options = NULL,
            bool hasSub = false);

    static SegmentDirectoryPtr Create(
            const file_system::DirectoryVector& directoryVec,
            bool hasSub = false);

    static SegmentDirectoryPtr Create(
            const file_system::DirectoryVector& directoryVec,
            const std::vector<index_base::Version>& versions,
            bool hasSub = false);

private:
    static SegmentDirectoryPtr Create(
        const config::IndexPartitionOptions* options, bool isParallelBuild = false);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDirectoryCreator);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_DIRECTORY_CREATOR_H
