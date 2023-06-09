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

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/FenceContext.h"

namespace indexlib { namespace file_system {
class PackageFileMeta;

class DirectoryMerger
{
public:
    DirectoryMerger() = delete;
    ~DirectoryMerger() = delete;

public:
    static ErrorCode MoveFiles(const fslib::EntryList& files, const std::string& targetDir);
    static FSResult<bool> MergePackageFiles(const std::string& dir,
                                            FenceContext* fenceContext = FenceContext::NoFence());

private:
    static void SplitFileName(const std::string& input, std::string& folder, std::string& fileName);
    static FSResult<fslib::EntryList> ListFiles(const std::string& dir, const std::string& subDir);

    static FSResult<bool> CollectPackageMetaFiles(const std::string& dir, fslib::FileList& metaFileNames,
                                                  FenceContext* fenceContext);
    static ErrorCode MergePackageMeta(const std::string& dir, const fslib::FileList& metaFileNames,
                                      FenceContext* fenceContext);
    static ErrorCode MovePackageDataFile(uint32_t baseFileId, const std::string& dir,
                                         const std::vector<std::string>& srcFileNames,
                                         const std::vector<std::string>& srcFileTags,
                                         std::vector<std::string>& newFileNames);
    static ErrorCode StorePackageFileMeta(const std::string& dir, PackageFileMeta& mergedMeta,
                                          FenceContext* fenceContext);
    static ErrorCode CleanMetaFiles(const std::string& dir, const fslib::FileList& metaFileNames,
                                    FenceContext* fenceContext);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DirectoryMerger> DirectoryMergerPtr;
}} // namespace indexlib::file_system
