#ifndef __INDEXLIB_LOCAL_INDEX_CLEANER_H
#define __INDEXLIB_LOCAL_INDEX_CLEANER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/executor.h"

DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, Version);

IE_NAMESPACE_BEGIN(partition);

class LocalIndexCleaner
{
public:
    LocalIndexCleaner(const file_system::DirectoryPtr& rootDir,
                      uint32_t keepVersionCount = INVALID_KEEP_VERSION_COUNT,
                      const ReaderContainerPtr& readerContainer = ReaderContainerPtr());
    ~LocalIndexCleaner() = default;
    
public:
    bool Clean(const std::vector<versionid_t>& keepVersionIds);

private:
    void DoClean(const std::vector<versionid_t>& keepVersionIds,
                 bool cleanAfterMaxKeepVersionFiles);
    static void CleanFiles(const file_system::DirectoryPtr& rootDir,
                           const fslib::FileList& fileList,
                           const std::set<index_base::Version>& keepVersions,
                           bool cleanAfterMaxKeepVersionFiles);
    static void CleanPatchIndexSegmentFiles(const file_system::DirectoryPtr& rootDir,
                                            const std::string& patchDirName,
                                            const std::set<segmentid_t>& keepSegmentIds,
                                            segmentid_t maxCanCleanSegmentId,
                                            schemavid_t patchSchemaId);
    const index_base::Version& GetVersion(versionid_t versionId);

private:
    mutable autil::ThreadMutex mLock;

    file_system::DirectoryPtr mRootDir;
    uint32_t mKeepVersionCount;
    ReaderContainerPtr mReaderContainer;
    std::set<index_base::Version> mVersionCache;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocalIndexCleaner);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_LOCAL_INDEX_CLEANER_H
