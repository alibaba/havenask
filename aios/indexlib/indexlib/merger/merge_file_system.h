#ifndef __INDEXLIB_MERGE_FILE_SYSTEM_H
#define __INDEXLIB_MERGE_FILE_SYSTEM_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/thread_local.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_system_options.h"
#include "indexlib/storage/archive_folder.h"
#include "indexlib/storage/raid_config.h"
#include "indexlib/config/merge_config.h"


DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);

IE_NAMESPACE_BEGIN(merger);

class MergeFileSystem
{
public:
    static MergeFileSystemPtr Create(const std::string& rootPath,
        const config::MergeConfig& mergeConfig, uint32_t instanceId,
        const storage::RaidConfigPtr& raidConfig);

public:
    MergeFileSystem(const std::string& rootPath, const config::MergeConfig& mergeConfig,
        uint32_t instanceId, const storage::RaidConfigPtr& raidConfig);
    ~MergeFileSystem();
    
public:
    virtual void Init(const std::vector<std::string>& targetSegmentPaths); // relativePath
    virtual void Close();
    virtual void Commit();
    virtual void MakeDirectory(const std::string& absolutePath, bool mayExist);
    file_system::DirectoryPtr GetDirectory(const std::string& absolutePath);
    const std::string& GetRootPath() { return mRootPath; }

    storage::ArchiveFolderPtr GetArchiveFolder(const std::string& absolutePath, 
                                               bool forceUseArchiveFile);

public:
    virtual void MakeCheckpoint(const std::string& fileName);
    virtual bool HasCheckpoint(const std::string& fileName);
    
protected:
    struct ThreadFileSystem{
        file_system::IndexlibFileSystemPtr fileSystem;
        std::string description;
    };

protected:
    virtual ThreadFileSystem* CreateFileSystem();

protected:
    const file_system::IndexlibFileSystemPtr& GetFileSystem();
    const std::string& GetDescription();
    ThreadFileSystem* GetThreadFileSystem();

protected:
    std::string mRootPath;
    file_system::FileSystemOptions mFileSystemOptions;
    config::MergeConfig mMergeConfig;
    misc::MetricProviderPtr mMetricProvider;
    std::map<std::string, storage::ArchiveFolderPtr> mArchiveFolders;
    autil::RecursiveThreadMutex mLock;
    uint32_t mInstanceId;

private:
    std::unique_ptr<util::ThreadLocalPtr> mThreadFileSystem;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeFileSystem);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_FILE_SYSTEM_H
