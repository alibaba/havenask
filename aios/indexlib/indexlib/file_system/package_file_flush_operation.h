#ifndef __INDEXLIB_PACKAGE_FILE_FLUSH_OPERATION_H
#define __INDEXLIB_PACKAGE_FILE_FLUSH_OPERATION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_flush_operation.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/package_file_meta.h"
#include "indexlib/file_system/file_system_define.h"

IE_NAMESPACE_BEGIN(file_system);

class PackageFileFlushOperation : public FileFlushOperation
{
public:
    PackageFileFlushOperation(
            const std::string& packageFilePath,
            const PackageFileMetaPtr& packageFileMeta,
            const std::vector<FileNodePtr>& fileNodeVec,
            const std::vector<FSDumpParam>& dumpParamVec,
            const storage::RaidConfigPtr& raidConfig);

    ~PackageFileFlushOperation();

public:
    void Execute(const PathMetaContainerPtr& pathMetaContainer) override;
    const std::string& GetPath() const override
    {  return mPackageFilePath; }
    void GetFileNodePaths(fslib::FileList& fileList) const override;
    
private:
    void StoreDataFile(const PathMetaContainerPtr& pathMetaContainer);
    void StoreMetaFile(const std::string& metaFilePath);
    void MakeFlushedFileNotDirty();
    bool NeedRaid() const;

private:
    std::string mPackageFilePath;
    PackageFileMetaPtr mPackageFileMeta;
    std::vector<FileNodePtr> mInnerFileNodes;
    std::vector<FileNodePtr> mFlushFileNodes;
    bool mAtomicDump;
    storage::RaidConfigPtr mRaidConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageFileFlushOperation);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGE_FILE_FLUSH_OPERATION_H
