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

#include <memory>

#include "autil/legacy/jsonizable.h"

namespace indexlib::file_system {

class FileListToPackage : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    std::vector<std::string> filePathList;
    std::vector<size_t> fileSizes;
    std::vector<size_t> alignedFileSizes;
    size_t totalPhysicalSize = 0;
    uint32_t dataFileIdx = 0;
    std::string packageTag;
};

class PackagingPlan : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    // The dst package file paths are logical paths relative to output segment.
    // The src unpackaged file paths are physical paths. Physical paths are necessary because src files might be under
    // different op fence directories.
    std::map<std::string, FileListToPackage>
        dstPath2SrcFilePathsMap; // new(dst) package file path -> unpackaged(src) file path
    // srcDirPaths are logical paths relative to segment directory. Dirs don't need to be physically moved/copies, they
    // will be created when files are generated. We keep this only to make sure even empty dirs get mapped to package.
    std::set<std::string> srcDirPaths;
};

} // namespace indexlib::file_system