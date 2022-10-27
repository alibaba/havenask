#ifndef __INDEXLIB_PACKAGE_FILE_UTIL_H
#define __INDEXLIB_PACKAGE_FILE_UTIL_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/package_file_writer.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/directory_merger.h"

IE_NAMESPACE_BEGIN(file_system);

class PackageFileUtil
{
public:
    PackageFileUtil() {} 
    ~PackageFileUtil() {} 

public:
    // packageFileInfoStr: filePath:fileContent#filePath:fileContent....
    // packageDirInfoStr: dirPath:dirPath....
    static void CreatePackageFile(
            const DirectoryPtr& directory,
            const std::string& packageFileName,
            const std::string& packageFileInfoStr,
            const std::string& packageDirInfoStr)
    {
        // TODO: support local directory
        InMemDirectoryPtr inMemDirectory = DYNAMIC_POINTER_CAST(
                InMemDirectory, directory);
        assert(inMemDirectory);

        PackageFileWriterPtr packageFileWriter = 
            directory->CreatePackageFileWriter(packageFileName);
        assert(packageFileWriter);

        CreatePackageFile(packageFileWriter, packageFileInfoStr, 
                          packageDirInfoStr); 
    }

    static void CreatePackageFile(
            const PackageFileWriterPtr& packageFileWriter,
            const std::string& packageFileInfoStr,
            const std::string& packageDirInfoStr)
    {
        std::vector<std::vector<std::string> > packageFileInfos;
        std::vector<std::string> packageDirInfos;

        autil::StringUtil::fromString(packageDirInfoStr, packageDirInfos, ":");
        for (size_t i = 0; i < packageDirInfos.size(); i++)
        {
            packageFileWriter->MakeInnerDir(packageDirInfos[i]);
        }

        autil::StringUtil::fromString(packageFileInfoStr, 
                packageFileInfos, ":", "#");
        for (size_t i = 0; i < packageFileInfos.size(); i++)
        {
            assert(packageFileInfos[i].size() == 2);
            FileWriterPtr innerFileWriter = 
                packageFileWriter->CreateInnerFileWriter(
                        packageFileInfos[i][0]);
            assert(innerFileWriter);

            innerFileWriter->Write(packageFileInfos[i][1].data(),
                    packageFileInfos[i][1].size());
            innerFileWriter->Close();
        }
        packageFileWriter->Close();
    }

    // packageFileInfoStr: filePath1:fileContent1:fileTag1#filePath1:fileContent1;filePath2:fileContent2....
    // packageDirInfoStr: dirPath:dirPath....
    static void CreatePackageFile(const std::string& rootPath,
                                  const std::string& relativeMountPath,
                                  const std::string& packageFileInfoStr,
                                  const std::string& packageDirInfoStr)
    {
        storage::FileSystemWrapper::MkDirIfNotExist(rootPath);
        auto fs = IndexlibFileSystemCreator::Create(rootPath);
        auto path = util::PathUtil::JoinPath(rootPath, relativeMountPath);
        fs->MountPackageStorage(path, "test");
        auto directory = DirectoryCreator::Create(fs, path);
        std::vector<std::string> packageDirInfos;
        autil::StringUtil::fromString(packageDirInfoStr, packageDirInfos, ":");
        for (size_t i = 0; i < packageDirInfos.size(); i++)
        {
            directory->MakeDirectory(packageDirInfos[i]);
        }
        
        std::vector<std::string> onePackageFileInfoStrs;
        autil::StringUtil::fromString(packageFileInfoStr, onePackageFileInfoStrs, ";");
        std::string emptyString;

        const PackageStoragePtr& storage = directory->GetFileSystem()->GetPackageStorage();
        for (const auto& onePackageFileInfoStr : onePackageFileInfoStrs)
        {
            std::vector<std::vector<std::string> > packageFileInfos;
            autil::StringUtil::fromString(onePackageFileInfoStr, packageFileInfos, ":", "#");
            for (const auto& packageFileInfo : packageFileInfos)
            {
                assert(packageFileInfo.size() == 2 || packageFileInfo.size() == 3);
                storage->SetTagFunction([&](const std::string& relativeFilePath){
                            return packageFileInfo.size() > 2 ? packageFileInfo[2] : emptyString;
                    });            
                const auto& fileWriter = directory->CreateFileWriter(packageFileInfo[0]);
                assert(fileWriter);
                fileWriter->Write(packageFileInfo[1].data(), packageFileInfo[1].size());
                fileWriter->Close();
            }
            storage->Commit();
        }
        storage->Close();
        bool ret = DirectoryMerger::MergePackageFiles(path);
        assert(ret); (void)ret;
    }


private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageFileUtil);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGE_FILE_UTIL_H
