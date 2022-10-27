#ifndef __INDEXLIB_FILE_SYSTEM_ADAPTER_H
#define __INDEXLIB_FILE_SYSTEM_ADAPTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(file_system);

template <typename PathType>
class FileSystemAdapter
{
public:
    static const std::string& GetPathName(const PathType& path);
    static void DeleteFile(const PathType& rootDir, const std::string& fileName);
    static void DeleteDir(const PathType& rootDir, const std::string& dirName);    
    static void DeleteFileIfExist(const PathType& rootDir, const std::string& fileName);
    static bool IsFileExist(const PathType& rootDir, const std::string& fileName);
    static PathType GetPath(const PathType& rootDir, const std::string& relativePath);
    static void ListDir(const PathType& rootDir, fslib::FileList& fileList,
                         bool recursive = false);
};

template <>
class FileSystemAdapter<std::string>
{
public:
    static const std::string& GetPathName(const std::string& path) { return path; }
    static void DeleteFile(const std::string& rootDir, const std::string& fileName)
    {
        storage::FileSystemWrapper::DeleteFile(
            storage::FileSystemWrapper::JoinPath(rootDir, fileName));         
    }
    static void DeleteDir(const std::string& rootDir, const std::string& dirName)
    {
        storage::FileSystemWrapper::DeleteFile(
            storage::FileSystemWrapper::JoinPath(rootDir, dirName)); 
    }    
    static void DeleteFileIfExist(const std::string& rootDir, const std::string& fileName)
    {
        storage::FileSystemWrapper::DeleteIfExist(
            storage::FileSystemWrapper::JoinPath(rootDir,  fileName));         
    }
    static bool IsFileExist(const std::string& rootDir, const std::string& fileName)
    {
        return storage::FileSystemWrapper::IsExist(
            storage::FileSystemWrapper::JoinPath(rootDir, fileName));        
    }

    static std::string GetPath(const std::string& rootDir, const std::string& relativePath)
    {
        return storage::FileSystemWrapper::JoinPath(rootDir, relativePath);
    }

    static void ListDir(const std::string& rootDir, fslib::FileList& fileList, bool recursive)
    {
        if (recursive)
        {
            storage::FileSystemWrapper::ListDir(rootDir, fileList);
        }
        else
        {
            storage::FileSystemWrapper::ListDirRecursive(rootDir, fileList);
        }
    }
};

template <>
class FileSystemAdapter<file_system::DirectoryPtr>
{
public:
    static const std::string& GetPathName(const file_system::DirectoryPtr& path)
    {
        return path->GetPath();
    }
    static void DeleteFile(const file_system::DirectoryPtr& rootDir, const std::string& fileName)
    {
        rootDir->RemoveFile(fileName);        
    }
    static void DeleteDir(const file_system::DirectoryPtr& rootDir, const std::string& dirName)
    {
        rootDir->RemoveDirectory(dirName); 
    }    
    static void DeleteFileIfExist(const file_system::DirectoryPtr& rootDir, const std::string& fileName)
    {
        try {
            rootDir->RemoveFile(fileName);
        }
        catch (const misc::NonExistException& e)
        {
            // do nothing
        }
    }
    static bool IsFileExist(const file_system::DirectoryPtr& rootDir, const std::string& fileName)
    {
        return rootDir->IsExist(fileName);
    }

    static file_system::DirectoryPtr GetPath(const file_system::DirectoryPtr& rootDir,
            const std::string& relativePath)
    {
        assert(rootDir);
        return rootDir->GetDirectory(relativePath, true);
    }

    static void ListDir(const file_system::DirectoryPtr& rootDir,
                        fslib::FileList& fileList, bool recursive)
    {
        assert(rootDir);
        rootDir->ListFile("", fileList, recursive, true);
    }

};


IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_SYSTEM_ADAPTER_H
