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
#pragma once

#include "autil/NoCopyable.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/package/MergePackageMeta.h"
#include "indexlib/file_system/package/PackagingPlan.h"

namespace indexlib::file_system {
class IDirectory;
class FileWriter;
class MergePackageUtil : public autil::NoCopyable
{
public:
    static constexpr char PACKAGING_PLAN_FILE_NAME[] = "packaging_plan";

public:
    // ConvertDirToPackage does not require the underlying input/output storage to be
    // PackageMemStorage/PackageDiskStorage. It will write output files compliant with package format.
    static FSResult<void> ConvertDirToPackage(const std::shared_ptr<IDirectory>& workingDirectory,
                                              const std::shared_ptr<IDirectory>& outputDirectory,
                                              const std::string& parentDirName,
                                              const MergePackageMeta& mergePackageMeta,
                                              uint32_t packagingThresholdBytes) noexcept;
    static FSResult<void> CleanSrcIndexFiles(const std::shared_ptr<IDirectory>& directoryToClean,
                                             const std::shared_ptr<IDirectory>& directoryWithPackagingPlan) noexcept;
    static FSResult<bool> IsPackageConversionDone(const std::shared_ptr<IDirectory>& directory);

public:
    static FSResult<void> LoadOrGeneratePackagingPlan(const std::shared_ptr<IDirectory>& outputDirectory,
                                                      const MergePackageMeta& mergePackageMeta,
                                                      int32_t packagingThresholdBytes, PackagingPlan* plan) noexcept;
    static FSResult<void> MoveOrCopyIndexToPackages(const std::shared_ptr<IDirectory>& currentFenceDirectory,
                                                    const std::shared_ptr<IDirectory>& outputDirectory,
                                                    const PackagingPlan& packagingPlan) noexcept;
    static FSResult<void> GeneratePackageMeta(const std::shared_ptr<IDirectory>& outputDirectory,
                                              const std::string& parentDirName,
                                              const PackagingPlan& packagingPlan) noexcept;

private:
    static FSResult<void> MoveFileToPackage(const std::string& srcPhysicalPath, size_t fileSize,
                                            const std::shared_ptr<IDirectory>& outputDirectory,
                                            const std::string& dstPath) noexcept;
    static FSResult<void> CopyFilesToPackage(const FileListToPackage& fileListToPackage,
                                             const std::shared_ptr<IDirectory>& currentFenceDirectory,
                                             const std::shared_ptr<IDirectory>& outputDirectory,
                                             const std::string& targetPackageFileName) noexcept;
    static FSResult<void> CopyAndAppend(const std::string& srcPhysicalPath, size_t fileSize, size_t alignedFileSize,
                                        const std::shared_ptr<FileWriter>& dstFileWriter, bool shouldAlign) noexcept;
    static void GeneratePackagingPlan(const MergePackageMeta& mergePackageMeta, int32_t packagingThresholdBytes,
                                      PackagingPlan* plan) noexcept;
    static FSResult<void> AlignPackageFile(const std::shared_ptr<FileWriter>& fileWriter, size_t fileSize,
                                           size_t alignedFileSize);
    static size_t GetAlignedFileSize(size_t fileSize);
    static void FinalizeFilstListToPackage(size_t packageIndex, const std::string& tag,
                                           FileListToPackage* fileListToPackage) noexcept;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::file_system
