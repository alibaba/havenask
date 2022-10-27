#ifndef __INDEXLIB_DIRECTORY_MERGER_H
#define __INDEXLIB_DIRECTORY_MERGER_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, PackageFileMeta);

IE_NAMESPACE_BEGIN(file_system);

class DirectoryMerger
{
public:
    DirectoryMerger();
    ~DirectoryMerger();
    
public:
    static void MoveFiles(const fslib::FileList &files, const std::string &targetDir);
    static bool MergePackageFiles(const std::string& dir);
    
private:
    static void SplitFileName(const std::string& input, std::string& folder, std::string& fileName);
    static void MkDirIfNotExist(const std::string& dir);
    static fslib::FileList ListFiles(const std::string& dir, const std::string& subDir);
    
    static bool CollectPackageMetaFiles(const std::string& dir, fslib::FileList& metaFileNames);
    static void MergePackageMeta(const std::string& dir, const fslib::FileList& metaFileNames);
    static void MovePackageDataFile(uint32_t baseFileId, const std::string& dir, 
                                    const std::vector<std::string>& srcFileNames,
                                    const std::vector<std::string>& srcFileTags,
                                    std::vector<std::string>& newFileNames);
    static void StorePackageFileMeta(const std::string& dir, file_system::PackageFileMeta& mergedMeta);
    static void CleanMetaFiles(const std::string& dir, const fslib::FileList& metaFileNames);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DirectoryMerger);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DIRECTORY_MERGER_H
