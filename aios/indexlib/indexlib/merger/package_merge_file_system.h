#ifndef __INDEXLIB_PACKAGE_MERGE_FILE_SYSTEM_H
#define __INDEXLIB_PACKAGE_MERGE_FILE_SYSTEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/merge_file_system.h"

IE_NAMESPACE_BEGIN(merger);

class PackageMergeFileSystem : public MergeFileSystem
{
public:
    PackageMergeFileSystem(const std::string& rootPath, const config::MergeConfig& mergeConfig,
        uint32_t instanceId, const storage::RaidConfigPtr& raidConfig);
    ~PackageMergeFileSystem();
    
public:
    void Init(const std::vector<std::string>& targetSegmentPaths) override;
    void MakeDirectory(const std::string& absolutePath, bool mayExist) override;
    void Commit() override;
public:
    void MakeCheckpoint(const std::string& fileName) override;
    bool HasCheckpoint(const std::string& fileName) override;

private:
    uint32_t GetThreadId();
    MergeFileSystem::ThreadFileSystem* CreateFileSystem() override;
    int64_t GetLastTimestamp();
    void SetLastTimestamp(int64_t timestamp);
    void AddThreadCheckpoint(const std::string& fileName);
    void PopThreadCheckpoints(std::vector<std::string>& destition);
    int32_t Commit(MergeFileSystem::ThreadFileSystem* tfs);

private:
    static const std::string PACKAGE_MERGE_CHECKPOINT;
    class CheckpointManager : public autil::legacy::Jsonizable
    {
    public:
        void Recover(const storage::ArchiveFolderPtr& folder, uint32_t threadCount);
        void MakeCheckpoint(const std::string& fileName);
        bool HasCheckpoint(const std::string& fileName) const;
        void Commit(const storage::ArchiveFolderPtr& folder,
                    const std::string& description, int32_t metaId);
        int32_t GetMetaId(const std::string& description) const;
        uint32_t GetThreadCount() const { return mThreadCount; }

    private:
        void Jsonize(JsonWrapper& json) override;
        void Store(const storage::ArchiveFolderPtr& folder);
        void Load(const storage::ArchiveFolderPtr& folder);

    private:
        std::set<std::string> mCheckpoints;
        std::map<std::string, int32_t> mDescriptionToMetaId;
        uint32_t mThreadCount;
    };
    
private:
    std::vector<std::string> mTargetSegmentPaths;
    std::map<std::string, fslib::FileList> mTargetSegmentFileList; // for optimize recover
    std::set<std::string> mNeedMakeDirectoryPaths;
    CheckpointManager mCheckpointManager;
    uint32_t mInstanceId;
    uint32_t mNextThreadId;
    autil::RecursiveThreadMutex mLock;
    std::unique_ptr<util::ThreadLocalPtr> mThreadLastTimestamp;
    std::unique_ptr<util::ThreadLocalPtr> mThreadCheckpoints;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageMergeFileSystem);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_PACKAGE_MERGE_FILE_SYSTEM_H
