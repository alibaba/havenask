#ifndef __INDEXLIB_PATH_META_CONTAINER_H
#define __INDEXLIB_PATH_META_CONTAINER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include <fslib/fslib.h>
#include <map>
#include <set>
#include <tr1/unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_system_define.h"

IE_NAMESPACE_BEGIN(file_system);

class PathMetaContainer
{
public:
    PathMetaContainer();
    ~PathMetaContainer();
    
public:
    bool AddFileInfo(
        const std::string& filePath, int64_t fileLength, uint64_t modifyTime, bool isDirectory);

    bool AddFileInfoForOneSolidPath(const std::string& solidPath,
                                    std::vector<FileInfo>::const_iterator firstFileInfo,
                                    std::vector<FileInfo>::const_iterator lastFileInfo);

    void MarkNotExistSolidPath(const std::string& solidPath);

    bool MatchSolidPath(const std::string& path) const;

    bool RemoveFile(const std::string& filePath);
    
    bool RemoveDirectory(const std::string& path);

    bool IsExist(const std::string& path) const;

    bool ListFile(const std::string& dirPath, fslib::FileList& fileList, bool recursive);

    bool GetFileInfo(const std::string& path, FileInfo& fileInfo) const;

    size_t GetFileLength(const std::string& filePath) const;

private:
    DEFINE_SHARED_PTR(FileInfo);
    typedef std::map<std::string, FileInfoPtr> FileInfoMap;
    typedef std::tr1::unordered_map<std::string, FileInfoPtr> FastFileInfoMap;

    // first : path, second : exist
    typedef std::map<std::string, bool> SolidPathMap;

    FileInfoMap mFileInfoMap;
    FastFileInfoMap mFastMap;
    SolidPathMap mSolidPathMap;
    mutable autil::RecursiveThreadMutex mLock;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PathMetaContainer);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PATH_META_CONTAINER_H
