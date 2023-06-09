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
#ifndef __INDEXLIB_FILE_SYSTEM_DIRECTORY_CREATOR_H
#define __INDEXLIB_FILE_SYSTEM_DIRECTORY_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"

DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);

namespace indexlib { namespace test {

class DirectoryCreator
{
public:
    DirectoryCreator();
    ~DirectoryCreator();

public:
    // old api, todo rm
    static file_system::DirectoryPtr Create(const file_system::IFileSystemPtr& fileSystem, const std::string& path,
                                            bool isAbsPath = true);

    static file_system::DirectoryPtr Get(const file_system::IFileSystemPtr& fileSystem, const std::string& path,
                                         bool throwExceptionIfNotExist, bool isAbsPath = true);

    /**
     * @param isOffline: this will create a OfflineFileSystem which is optimized for remote FileSystem access
     **/
    static file_system::DirectoryPtr Create(const std::string& absPath, bool useCache = false, bool isOffline = false);

    static file_system::DirectoryPtr Create(const std::string& absPath,
                                            const file_system::FileSystemOptions& fsOptions);

public:
    // new api
    // dirPath = "" for GetRootDirectory
    static file_system::DirectoryPtr GetDirectory(const file_system::IFileSystemPtr& fileSystem,
                                                  const std::string& dirPath = "");
    static file_system::DirectoryPtr CreateOfflineRootDir(const std::string& rootPath);

private:
    static file_system::DirectoryPtr CreateDirectory(const file_system::IFileSystemPtr& fileSystem,
                                                     const std::string& path, bool isAbsPath);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DirectoryCreator);
}} // namespace indexlib::test

#endif //__INDEXLIB_FILE_SYSTEM_DIRECTORY_CREATOR_H
