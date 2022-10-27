#ifndef __INDEXLIB_PARTITION_PATCH_INDEX_ACCESSOR_H
#define __INDEXLIB_PARTITION_PATCH_INDEX_ACCESSOR_H

#include <tr1/memory>
#include "fslib/common/common_type.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index_base);

class PartitionPatchIndexAccessor
{
public:
    PartitionPatchIndexAccessor();
    ~PartitionPatchIndexAccessor();

public:
    void Init(const file_system::DirectoryPtr& rootDir,
              const index_base::Version& version);

    void SetSubSegmentDir() { mIsSub = true; }
    
    file_system::DirectoryPtr GetIndexDirectory(segmentid_t segmentId,
            const std::string& indexName, bool throwExceptionIfNotExist);

    file_system::DirectoryPtr GetAttributeDirectory(segmentid_t segmentId,
            const std::string& attrName, bool throwExceptionIfNotExist);

    file_system::DirectoryPtr GetSectionAttributeDirectory(segmentid_t segmentId,
            const std::string& indexName, bool throwExceptionIfNotExist);

    versionid_t GetVersionId() const { return mVersion.GetVersionId(); }
    
    const PartitionPatchMeta& GetPartitionPatchMeta() const { return mPatchMeta; }
    const file_system::DirectoryPtr& GetRootDirectory() const { return mRootDir; }
    const index_base::Version& GetVersion() const { return mVersion; }
    
public:
    static std::string GetPatchRootDirName(schemavid_t schemaId);
    static bool ExtractSchemaIdFromPatchRootDir(const std::string& rootDir,
            schemavid_t& schemaId);
    
    static void ListPatchRootDirs(const std::string& rootDir,
                                  fslib::FileList& patchRootList);

    static void ListPatchRootDirs(const file_system::DirectoryPtr& rootDir,
                                  fslib::FileList& patchRootList, bool localOnly = false);
    
private:
    PartitionPatchMeta mPatchMeta;
    index_base::Version mVersion;
    file_system::DirectoryPtr mRootDir;
    bool mIsSub;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionPatchIndexAccessor);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PARTITION_PATCH_INDEX_ACCESSOR_H
