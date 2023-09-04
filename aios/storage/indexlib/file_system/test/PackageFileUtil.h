#pragma once

#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/DirectoryMerger.h"
#include "indexlib/file_system/package/PackageDiskStorage.h"
#include "indexlib/file_system/package/PackageFileWriter.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {

class PackageFileUtil
{
public:
    PackageFileUtil() {}
    ~PackageFileUtil() {}

public:
    // packageFileInfoStr: filePath:fileContent#filePath:fileContent....
    // packageDirInfoStr: dirPath:dirPath....
    static void CreatePackageFile(const std::shared_ptr<Directory>& directory, const std::string& packageFileInfoStr,
                                  const std::string& packageDirInfoStr)
    {
        std::vector<std::vector<std::string>> packageFileInfos;
        std::vector<std::string> packageDirInfos;

        autil::StringUtil::fromString(packageDirInfoStr, packageDirInfos, ":");
        for (size_t i = 0; i < packageDirInfos.size(); i++) {
            directory->MakeDirectory(packageDirInfos[i]);
        }

        autil::StringUtil::fromString(packageFileInfoStr, packageFileInfos, ":", "#");
        for (size_t i = 0; i < packageFileInfos.size(); i++) {
            assert(packageFileInfos[i].size() == 2);

            directory->MakeDirectory(util::PathUtil::GetParentDirPath(packageFileInfos[i][0]));

            WriterOption writerOption;
            auto fileWriter = directory->CreateFileWriter(packageFileInfos[i][0], writerOption);
            assert(fileWriter);

            fileWriter->Write(packageFileInfos[i][1].data(), packageFileInfos[i][1].size()).GetOrThrow();
            fileWriter->Close().GetOrThrow();
        }
    }

    // packageFileInfoStr: filePath1:fileContent1:fileTag1#filePath1:fileContent1;filePath2:fileContent2....
    // packageDirInfoStr: dirPath:dirPath....
    static void CreatePackageFile(const std::string& rootPath, const std::string& packageHintDirPath,
                                  const std::string& packageFileInfoStr, const std::string& packageDirInfoStr)
    {
        FileSystemOptions fsOptions = FileSystemOptions::Offline();
        fsOptions.outputStorage = FSST_PACKAGE_DISK;
        std::shared_ptr<IFileSystem> fs =
            FileSystemCreator::CreateForWrite("package_file_util", rootPath, fsOptions).GetOrThrow();
        std::shared_ptr<Directory> directory =
            Directory::Get(fs)->MakeDirectory(packageHintDirPath, DirectoryOption::Package());
        std::vector<std::string> packageDirInfos;
        autil::StringUtil::fromString(packageDirInfoStr, packageDirInfos, ":");
        for (size_t i = 0; i < packageDirInfos.size(); i++) {
            directory->MakeDirectory(packageDirInfos[i]);
        }

        std::vector<std::string> onePackageFileInfoStrs;
        autil::StringUtil::fromString(packageFileInfoStr, onePackageFileInfoStrs, ";");
        std::string emptyString;

        PackageDiskStoragePtr storage = std::dynamic_pointer_cast<PackageDiskStorage>(fs->TEST_GetOutputStorage());
        for (const std::string& onePackageFileInfoStr : onePackageFileInfoStrs) {
            std::vector<std::vector<std::string>> packageFileInfos;
            autil::StringUtil::fromString(onePackageFileInfoStr, packageFileInfos, ":", "#");
            for (const std::vector<std::string>& packageFileInfo : packageFileInfos) {
                assert(packageFileInfo.size() == 2 || packageFileInfo.size() == 3);
                storage->_tagFunction = [&](const std::string& relativeFilePath) {
                    return packageFileInfo.size() > 2 ? packageFileInfo[2] : emptyString;
                };
                directory->MakeDirectory(util::PathUtil::GetParentDirPath(packageFileInfo[0]));
                const auto& fileWriter = directory->CreateFileWriter(packageFileInfo[0]);
                assert(fileWriter);
                fileWriter->Write(packageFileInfo[1].data(), packageFileInfo[1].size()).GetOrThrow();
                fileWriter->Close().GetOrThrow();
            }
            [[maybe_unused]] auto ret = fs->CommitPackage();
            assert(ret.OK());
        }
        [[maybe_unused]] auto ret =
            DirectoryMerger::MergePackageFiles(util::PathUtil::JoinPath(rootPath, packageHintDirPath));
        assert(ret.OK());
    }

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PackageFileUtil> PackageFileUtilPtr;
}} // namespace indexlib::file_system
