/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/file_system/package/MergePackageUtil.h"

#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/util/PathUtil.h"
namespace indexlib::file_system {
namespace {
using util::PathUtil;
}
AUTIL_LOG_SETUP(indexlib.file_system, MergePackageUtil);

// workingDirectory is the directory which contains temporary files during merge
// outputDirectory is the directory which contains the merged files
// parentDirName is the name of the directory to be merged, e.g. segment_0_level_0. It's used to get relative path from
// the physical path in mergePackageMeta.
FSResult<void> MergePackageUtil::ConvertDirToPackage(const std::shared_ptr<IDirectory>& workingDirectory,
                                                     const std::shared_ptr<IDirectory>& outputDirectory,
                                                     const std::string& parentDirName,
                                                     const MergePackageMeta& mergePackageMeta,
                                                     uint32_t packagingThresholdBytes) noexcept
{
    AUTIL_LOG(INFO, "merge dir[%s] to package format, threshold bytes[%u]", parentDirName.c_str(),
              packagingThresholdBytes);
    const std::string PACKAGE_META_FILE_NAME = std::string(PACKAGE_FILE_PREFIX) + std::string(PACKAGE_FILE_META_SUFFIX);
    auto [ec, isExist] = outputDirectory->IsExist(PACKAGE_META_FILE_NAME).CodeWith();
    RETURN_IF_FS_ERROR(ec, "check package meta file exist failed");
    if (isExist) {
        return FSEC_OK;
    }
    PackagingPlan packagingPlan;
    RETURN_IF_FS_ERROR(
        LoadOrGeneratePackagingPlan(outputDirectory, mergePackageMeta, packagingThresholdBytes, &packagingPlan),
        "load or generate packaging plan failed");
    AUTIL_LOG(INFO, "load or generate packaging plan success, output directory[%s], packagingPlan[%s]",
              outputDirectory->GetPhysicalPath("").c_str(), autil::legacy::ToJsonString(packagingPlan).c_str());
    RETURN_IF_FS_ERROR(MoveOrCopyIndexToPackages(workingDirectory, outputDirectory, packagingPlan),
                       "move or copy files to packages failed, output directory[%s]",
                       outputDirectory->GetPhysicalPath("").c_str());
    RETURN_IF_FS_ERROR(GeneratePackageMeta(outputDirectory, parentDirName, packagingPlan),
                       "generate package meta failed, output directory[%s]",
                       outputDirectory->GetPhysicalPath("").c_str());
    return FSEC_OK;
}

void MergePackageUtil::FinalizeFilstListToPackage(size_t packageIndex, const std::string& tag,
                                                  FileListToPackage* fileListToPackage) noexcept
{
    const std::vector<size_t>& fileSizes = fileListToPackage->fileSizes;
    size_t totalPhysicalSize = 0;
    for (size_t i = 0; i < fileSizes.size(); ++i) {
        size_t alignedFileSize = GetAlignedFileSize(fileSizes[i]);
        fileListToPackage->alignedFileSizes.push_back(alignedFileSize);
        totalPhysicalSize += (i == fileSizes.size() - 1) ? fileSizes[i] : GetAlignedFileSize(fileSizes[i]);
    }
    fileListToPackage->totalPhysicalSize = totalPhysicalSize;
    fileListToPackage->dataFileIdx = packageIndex;
    fileListToPackage->packageTag = tag;
}
// inputList:  map src file physical path -> size in bytes
void MergePackageUtil::GeneratePackagingPlan(const MergePackageMeta& mergePackageMeta, int32_t packagingThresholdBytes,
                                             PackagingPlan* plan) noexcept
{
    size_t packageIndex = 0;
    for (const auto& [tag, srcFileList] : mergePackageMeta.packageTag2File2SizeMap) {
        // First make all files that are larger than threshold to be packaged separately.
        for (const auto& [srcFilePhysicalPath, fileSize] : srcFileList) {
            if (fileSize < packagingThresholdBytes) {
                continue;
            }
            FileListToPackage& fileListToPackage =
                plan->dstPath2SrcFilePathsMap[PackageFileMeta::GetPackageFileDataPath(PACKAGE_FILE_PREFIX, tag,
                                                                                      packageIndex)];
            fileListToPackage.filePathList.push_back(srcFilePhysicalPath);
            fileListToPackage.fileSizes.push_back(fileSize);
            FinalizeFilstListToPackage(packageIndex, tag, &fileListToPackage);
            packageIndex++;
        }
        // Then pack all files that are smaller than threshold into packages, each package will not exceed threshold.
        int32_t currentPackageSize = 0;
        for (const auto& [srcFilePhysicalPath, fileSize] : srcFileList) {
            if (fileSize >= packagingThresholdBytes) {
                continue;
            }
            size_t alignedFileSize = GetAlignedFileSize(fileSize);
            if (currentPackageSize + fileSize > packagingThresholdBytes) {
                FileListToPackage& fileListToPackage =
                    plan->dstPath2SrcFilePathsMap[PackageFileMeta::GetPackageFileDataPath(PACKAGE_FILE_PREFIX, tag,
                                                                                          packageIndex)];
                FinalizeFilstListToPackage(packageIndex, tag, &fileListToPackage);
                currentPackageSize = alignedFileSize;
                packageIndex++;
                FileListToPackage& newFileListToPackage =
                    plan->dstPath2SrcFilePathsMap[PackageFileMeta::GetPackageFileDataPath(PACKAGE_FILE_PREFIX, tag,
                                                                                          packageIndex)];
                newFileListToPackage.filePathList.push_back(srcFilePhysicalPath);
                newFileListToPackage.fileSizes.push_back(fileSize);
            } else {
                FileListToPackage& fileListToPackage =
                    plan->dstPath2SrcFilePathsMap[PackageFileMeta::GetPackageFileDataPath(PACKAGE_FILE_PREFIX, tag,
                                                                                          packageIndex)];
                currentPackageSize += alignedFileSize;
                fileListToPackage.filePathList.push_back(srcFilePhysicalPath);
                fileListToPackage.fileSizes.push_back(fileSize);
            }
        }
        // Handle the last package.
        if (currentPackageSize > 0) {
            FileListToPackage& fileListToPackage =
                plan->dstPath2SrcFilePathsMap[PackageFileMeta::GetPackageFileDataPath(PACKAGE_FILE_PREFIX, tag,
                                                                                      packageIndex)];
            FinalizeFilstListToPackage(packageIndex, tag, &fileListToPackage);
            packageIndex++;
        }
    }

    for (const std::string& path : mergePackageMeta.dirSet) {
        std::vector<std::string> possiblePaths = PathUtil::GetPossibleParentPaths(path, /*includeSelf=*/true);
        std::copy(possiblePaths.begin(), possiblePaths.end(),
                  std::inserter(plan->srcDirPaths, plan->srcDirPaths.end()));
    }
}

FSResult<void> MergePackageUtil::LoadOrGeneratePackagingPlan(const std::shared_ptr<IDirectory>& outputDirectory,
                                                             const MergePackageMeta& mergePackageMeta,
                                                             int32_t packagingThresholdBytes,
                                                             PackagingPlan* plan) noexcept
{
    auto [ec, isExist] = outputDirectory->IsExist(PACKAGING_PLAN_FILE_NAME).CodeWith();
    RETURN_IF_FS_ERROR(ec, "check packaging plan file existence failed, directory: %s",
                       outputDirectory->GetPhysicalPath("").c_str());
    if (isExist) {
        std::string content;
        RETURN_IF_FS_ERROR(
            outputDirectory->Load(PACKAGING_PLAN_FILE_NAME, ReaderOption::PutIntoCache(FSOT_MEM), content),
            "load packaging plan file failed, directory: %s", outputDirectory->GetPhysicalPath("").c_str());
        try {
            autil::legacy::FromJsonString(*plan, content);
            return FSEC_OK;
        } catch (const std::exception& e) {
            // If fails to load the plan, we will regenerate it.
            RETURN_IF_FS_ERROR(outputDirectory->RemoveFile(PACKAGING_PLAN_FILE_NAME, RemoveOption()),
                               "remove packaging plan file failed, directory: %s",
                               outputDirectory->GetPhysicalPath("").c_str());
        }
    }
    GeneratePackagingPlan(mergePackageMeta, packagingThresholdBytes, plan);
    std::string content = autil::legacy::ToJsonString(*plan);
    RETURN_IF_FS_ERROR(outputDirectory->Store(PACKAGING_PLAN_FILE_NAME, content, WriterOption::AtomicDump()),
                       "store packaging plan file failed, directory: %s", outputDirectory->GetPhysicalPath("").c_str());
    return FSEC_OK;
}

// This method should be reentrant in case of failure.
FSResult<void> MergePackageUtil::MoveFileToPackage(const std::string& srcPhysicalPath, size_t fileSize,
                                                   const std::shared_ptr<IDirectory>& outputDirectory,
                                                   const std::string& dstPath) noexcept
{
    std::string dstPhysicalPath = PathUtil::JoinPath(outputDirectory->GetPhysicalPath(""), dstPath);

    auto [ec, srcExist] = FslibWrapper::IsExist(srcPhysicalPath).CodeWith();
    RETURN_IF_FS_ERROR(ec, "is exist for src file[%s] failed", srcPhysicalPath.c_str());
    // maybe src is already moved, handle reentrant case
    if (!srcExist) {
        auto [ec1, dstExist] = outputDirectory->IsExist(dstPath).CodeWith();
        RETURN_IF_FS_ERROR(ec1, "is exist for dst file[%s] failed", dstPhysicalPath.c_str());
        if (!dstExist) {
            AUTIL_LOG(ERROR, "both src file[%s] and dst file[%s] are lost", srcPhysicalPath.c_str(),
                      dstPhysicalPath.c_str());
            return FSEC_ERROR;
        }
        return FSEC_OK;
    }
    RETURN_IF_FS_ERROR(FslibWrapper::Rename(srcPhysicalPath, dstPhysicalPath), "rename file from [%s] to [%s] failed",
                       srcPhysicalPath.c_str(), dstPhysicalPath.c_str());
    return FSEC_OK;
}

size_t MergePackageUtil::GetAlignedFileSize(size_t fileSize)
{
    static const size_t FILE_ALIGN_SIZE = getpagesize();
    const size_t remainder = fileSize % FILE_ALIGN_SIZE;
    if (remainder == 0) {
        return fileSize;
    }
    return fileSize + FILE_ALIGN_SIZE - remainder;
}

FSResult<void> MergePackageUtil::AlignPackageFile(const std::shared_ptr<FileWriter>& fileWriter, size_t fileSize,
                                                  size_t alignedFileSize)
{
    if (alignedFileSize == fileSize) {
        return FSEC_OK;
    }
    size_t paddingLen = alignedFileSize - fileSize;
    std::vector<uint8_t> paddingBuffer(paddingLen, 0);
    auto [ec, writeSize] = fileWriter->Write(paddingBuffer.data(), paddingLen);
    RETURN_IF_FS_ERROR(ec, "write padding buffer to [%s] failed", fileWriter->GetPhysicalPath().c_str());
    assert(writeSize == paddingLen);
    (void)writeSize;
    return FSEC_OK;
}

FSResult<void> MergePackageUtil::CopyAndAppend(const std::string& srcPhysicalPath, size_t fileSize,
                                               size_t alignedFileSize, const std::shared_ptr<FileWriter>& dstFileWriter,
                                               bool shouldAlign) noexcept
{
    constexpr int COPY_BUFFER_SIZE = 2 * 1024 * 1024; // 2MB
    std::vector<uint8_t> copyBuffer;
    copyBuffer.resize(COPY_BUFFER_SIZE);
    char* buffer = (char*)copyBuffer.data();

    std::shared_ptr<fslib::fs::File> file(fslib::fs::FileSystem::openFile(srcPhysicalPath, fslib::READ));
    if (file == nullptr) {
        AUTIL_LOG(ERROR, "open file failed, file[%s]", srcPhysicalPath.c_str());
        return FSEC_ERROR;
    }
    if (!file->isOpened()) {
        fslib::ErrorCode ec = file->getLastError();
        if (ec == fslib::EC_NOENT) {
            AUTIL_LOG(ERROR, "file not exist [%s].", srcPhysicalPath.c_str());
            return FSEC_NOENT;
        }
        AUTIL_LOG(ERROR, "file not exist [%s]. error: %s", srcPhysicalPath.c_str(),
                  FslibWrapper::GetErrorString(file->getLastError()).c_str());
        return FSEC_ERROR;
    }
    ssize_t readTotal = 0;
    ssize_t readSize = 0;
    do {
        readSize = file->read(buffer, COPY_BUFFER_SIZE);
        if (readSize > 0) {
            auto [ec, writeSize] = dstFileWriter->Write(buffer, readSize).CodeWith();
            RETURN_IF_FS_ERROR(ec, "copy from file[%s] failed", srcPhysicalPath.c_str());
            assert(writeSize == readSize);
            (void)writeSize;
            readTotal += readSize;
        }
    } while (readSize == COPY_BUFFER_SIZE);
    assert(readTotal == fileSize);
    if (!file->isEof()) {
        AUTIL_LOG(ERROR, "copy from file[%s] FAILED, error: [%s]", srcPhysicalPath.c_str(),
                  FslibWrapper::GetErrorString(file->getLastError()).c_str());
        return FSEC_ERROR;
    }
    fslib::ErrorCode ret = file->close();
    if (ret != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "Close file[%s] FAILED, error: [%s]", srcPhysicalPath.c_str(),
                  FslibWrapper::GetErrorString(ret).c_str());
        return ParseFromFslibEC(ret);
    }
    if (shouldAlign) {
        RETURN_IF_FS_ERROR(AlignPackageFile(dstFileWriter, fileSize, alignedFileSize), "align package file [%s] failed",
                           srcPhysicalPath.c_str());
    }
    return FSEC_OK;
}

FSResult<void> MergePackageUtil::CopyFilesToPackage(const FileListToPackage& fileListToPackage,
                                                    const std::shared_ptr<IDirectory>& currentFenceDirectory,
                                                    const std::shared_ptr<IDirectory>& outputDirectory,
                                                    const std::string& targetPackageFileName) noexcept
{
    // Handle the case of failure recovery that the package file already exists.
    auto [ec1, dstExist] = outputDirectory->IsExist(targetPackageFileName).CodeWith();
    RETURN_IF_FS_ERROR(ec1, "is exist for [%s] failed", targetPackageFileName.c_str());
    // Maybe the package file already exists, handle the reentrant case.
    if (dstExist) {
        auto [ec, size] = outputDirectory->GetFileLength(targetPackageFileName).CodeWith();
        RETURN_IF_FS_ERROR(ec, "get file[%s] length failed in dir [%s] ", targetPackageFileName.c_str(),
                           outputDirectory->GetPhysicalPath("").c_str());
        if (size == fileListToPackage.totalPhysicalSize) {
            AUTIL_LOG(INFO, "package file [%s] already exist and is intact, skip copy", targetPackageFileName.c_str());
            return FSEC_OK;
        }
        RETURN_IF_FS_ERROR(outputDirectory->RemoveFile(targetPackageFileName, RemoveOption()),
                           "remove file [%s] failed", targetPackageFileName.c_str());
    }
    auto [ec2, fileWriter] =
        currentFenceDirectory->CreateFileWriter(targetPackageFileName, WriterOption::BufferAtomicDump()).CodeWith();
    RETURN_IF_FS_ERROR(ec2, "create file writer for [%s] failed", targetPackageFileName.c_str());
    for (size_t i = 0; i < fileListToPackage.filePathList.size(); i++) {
        const std::string& srcFilePath = fileListToPackage.filePathList[i];
        RETURN_IF_FS_ERROR(CopyAndAppend(srcFilePath, fileListToPackage.fileSizes[i],
                                         fileListToPackage.alignedFileSizes[i], fileWriter,
                                         /*shouldAlign=*/i != fileListToPackage.filePathList.size() - 1),
                           "copy file [%s] failed", srcFilePath.c_str());
    }
    RETURN_IF_FS_ERROR(fileWriter->Close(), "close file writer for [%s] failed", targetPackageFileName.c_str());
    std::string srcPhysicalPath = currentFenceDirectory->GetPhysicalPath(targetPackageFileName);
    std::string targetPhysicalPath = PathUtil::JoinPath(outputDirectory->GetPhysicalPath(""), targetPackageFileName);
    RETURN_IF_FS_ERROR(FslibWrapper::Rename(srcPhysicalPath, targetPhysicalPath), "copy files from [%s] to [%s] failed",
                       srcPhysicalPath.c_str(), targetPhysicalPath.c_str());
    return FSEC_OK;
}

FSResult<void> MergePackageUtil::MoveOrCopyIndexToPackages(const std::shared_ptr<IDirectory>& currentFenceDirectory,
                                                           const std::shared_ptr<IDirectory>& outputDirectory,
                                                           const PackagingPlan& packagingPlan) noexcept
{
    for (const auto& [dstPackageFileName, fileListToPackage] : packagingPlan.dstPath2SrcFilePathsMap) {
        const std::vector<std::string>& fileList = fileListToPackage.filePathList;
        assert(!fileList.empty());
        if (fileList.size() == 1) {
            RETURN_IF_FS_ERROR(MoveFileToPackage(fileList.at(0), fileListToPackage.fileSizes.at(0), outputDirectory,
                                                 dstPackageFileName),
                               "move file[%s] to package[%s] failed", fileList.at(0).c_str(),
                               dstPackageFileName.c_str());
            continue;
        }
        RETURN_IF_FS_ERROR(
            CopyFilesToPackage(fileListToPackage, currentFenceDirectory, outputDirectory, dstPackageFileName),
            "copy files to package[%s] failed", dstPackageFileName.c_str());
    }
    return FSEC_OK;
}

// We might want to substract part of the src paths from packaging plan using @parentDirName. This is because packaging
// plan's src paths are relative to input root directory and might come from multiple srcs during merge. For N-to-1
// merge, the result package meta only need part of the src paths.
// e.g. src paths are index files from different ops 123/segment_0_level_0/index/index_name/posting, we want to merge
// all these files into output segment_0_level_0 dir.
// In this case we set parentDirName to segment_0_level_0, and the result package meta will only contain file path e.g.
// index/index_name/posting
FSResult<void> MergePackageUtil::GeneratePackageMeta(const std::shared_ptr<IDirectory>& outputDirectory,
                                                     const std::string& parentDirName,
                                                     const PackagingPlan& packagingPlan) noexcept
{
    PackageFileMeta packageFileMeta;
    packageFileMeta.SetFileAlignSize(getpagesize());
    // sortedByDataFileIdx is needed when adding physical package files to ensure package file name and length are at
    // same index within package file meta. This is required by package file meta format.
    std::vector<std::pair<std::string, FileListToPackage>> sortedByDataFileIdx;
    std::copy(packagingPlan.dstPath2SrcFilePathsMap.begin(), packagingPlan.dstPath2SrcFilePathsMap.end(),
              std::back_inserter<std::vector<std::pair<std::string, FileListToPackage>>>(sortedByDataFileIdx));
    std::sort(sortedByDataFileIdx.begin(), sortedByDataFileIdx.end(),
              [](const auto& l, const auto& r) { return l.second.dataFileIdx < r.second.dataFileIdx; });

    for (const auto& [packageFilePath, fileListToPackage] : sortedByDataFileIdx) {
        assert(fileListToPackage.filePathList.size() == fileListToPackage.fileSizes.size());
        size_t offset = 0;
        for (int i = 0; i < fileListToPackage.filePathList.size(); i++) {
            std::string pathUnderSegment =
                PathUtil::GetFilePathUnderParam(fileListToPackage.filePathList[i], parentDirName);
            InnerFileMeta innerFileMeta(/*path=*/pathUnderSegment,
                                        /*isDir=*/false);
            innerFileMeta.Init(/*offset=*/offset, /*length=*/fileListToPackage.fileSizes[i],
                               /*fileIdx=*/fileListToPackage.dataFileIdx);
            offset += fileListToPackage.alignedFileSizes[i];
            packageFileMeta.AddInnerFile(innerFileMeta);
        }
        packageFileMeta.AddPhysicalFile(packageFilePath, fileListToPackage.packageTag);
    }
    for (const std::string& dirName : packagingPlan.srcDirPaths) {
        InnerFileMeta innerFileMeta(/*path=*/dirName,
                                    /*isDir=*/true);
        innerFileMeta.Init(/*offset=*/0, /*length=*/0,
                           /*fileIdx=*/0);
        packageFileMeta.AddInnerFile(innerFileMeta);
    }
    return packageFileMeta.Store(outputDirectory);
}

FSResult<void>
MergePackageUtil::CleanSrcIndexFiles(const std::shared_ptr<IDirectory>& directoryToClean,
                                     const std::shared_ptr<IDirectory>& directoryWithPackagingPlan) noexcept
{
    auto [ec, isExist] = directoryWithPackagingPlan->IsExist(PACKAGING_PLAN_FILE_NAME).CodeWith();
    RETURN_IF_FS_ERROR(ec, "check packaging plan file existence failed, directory: %s",
                       directoryWithPackagingPlan->GetPhysicalPath("").c_str());
    if (!isExist) {
        AUTIL_LOG(ERROR, "packaging plan file does not exist, directory: %s",
                  directoryWithPackagingPlan->GetPhysicalPath("").c_str());
        return FSEC_UNKNOWN;
    }
    std::string content;
    RETURN_IF_FS_ERROR(
        directoryWithPackagingPlan->Load(PACKAGING_PLAN_FILE_NAME, ReaderOption::PutIntoCache(FSOT_MEM), content),
        "load packaging plan file failed, directory: %s", directoryWithPackagingPlan->GetPhysicalPath("").c_str());
    PackagingPlan plan;
    try {
        autil::legacy::FromJsonString(plan, content);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "parse packaging plan file failed, directory: %s, error: %s",
                  directoryWithPackagingPlan->GetPhysicalPath("").c_str(), e.what());
        return FSEC_UNKNOWN;
    }
    for (const auto& [dstPackageFileName, fileListToPackage] : plan.dstPath2SrcFilePathsMap) {
        for (const std::string& srcFilePath : fileListToPackage.filePathList) {
            auto [ec, isExist] = FslibWrapper::IsExist(srcFilePath).CodeWith();
            RETURN_IF_FS_ERROR(ec, "check src file[%s] existence failed", srcFilePath.c_str());
            if (!isExist) {
                AUTIL_LOG(DEBUG, "src file[%s] does not exist, skip", srcFilePath.c_str());
                continue;
            }
            RETURN_IF_FS_ERROR(FslibWrapper::DeleteFile(srcFilePath, DeleteOption()), "remove src file[%s] failed",
                               srcFilePath.c_str());
        }
    }
    for (const std::string& srcDirPath : plan.srcDirPaths) {
        auto [ec, isExist] = directoryToClean->IsExist(srcDirPath).CodeWith();
        RETURN_IF_FS_ERROR(ec, "check src dir[%s] existence failed", srcDirPath.c_str());
        if (!isExist) {
            continue;
        }
        RETURN_IF_FS_ERROR(directoryToClean->RemoveDirectory(srcDirPath, RemoveOption()), "remove src dir[%s] failed",
                           srcDirPath.c_str());
    }

    return FSEC_OK;
}

FSResult<bool> MergePackageUtil::IsPackageConversionDone(const std::shared_ptr<IDirectory>& directory)
{
    const std::string PACKAGE_META_FILE_NAME = std::string(PACKAGE_FILE_PREFIX) + std::string(PACKAGE_FILE_META_SUFFIX);
    auto [ec, isExist] = directory->IsExist(PACKAGE_META_FILE_NAME).CodeWith();
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "check package meta file exist failed, directory: %s, ec[%d]",
                  directory->GetPhysicalPath("").c_str(), ec);
        return {ec, false};
    }
    return {FSEC_OK, isExist};
}

} // namespace indexlib::file_system
