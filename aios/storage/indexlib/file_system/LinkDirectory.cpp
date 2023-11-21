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
#include "indexlib/file_system/LinkDirectory.h"

#include <assert.h>

#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, LinkDirectory);

LinkDirectory::LinkDirectory(const std::string& path, const std::shared_ptr<IFileSystem>& fileSystem)
    : LocalDirectory(path, fileSystem)
{
    assert(path.find(FILE_SYSTEM_ROOT_LINK_NAME) != std::string::npos);
    assert(GetFileSystem()->IsExist(path).GetOrThrow());
}

LinkDirectory::~LinkDirectory() {}

FSResult<std::shared_ptr<IDirectory>> LinkDirectory::MakeDirectory(const std::string& dirPath,
                                                                   const DirectoryOption& directoryOption) noexcept
{
    AUTIL_LOG(ERROR, "LinkDirectory [%s] not support MakeDirectory", GetRootDir().c_str());
    return {FSEC_NOTSUP, std::shared_ptr<IDirectory>()};
}

FSResult<std::shared_ptr<IDirectory>> LinkDirectory::GetDirectory(const std::string& dirPath) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), dirPath);
    auto [ec, exist] = GetFileSystem()->IsExist(absPath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IDirectory>(), "IsExist [%s] failed", absPath.c_str());
    if (!exist) {
        AUTIL_LOG(DEBUG, "directory [%s] not exist", absPath.c_str());
        return {FSEC_NOENT, std::shared_ptr<IDirectory>()};
    }
    auto [ec2, isDir] = GetFileSystem()->IsDir(absPath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IDirectory>(), "IsDir [%s] failed", absPath.c_str());
    if (!isDir) {
        AUTIL_LOG(ERROR, "[%s] does exist. but it is not directory.", absPath.c_str());
        return {FSEC_NOTSUP, std::shared_ptr<IDirectory>()};
    }
    return {FSEC_OK, std::make_shared<LinkDirectory>(absPath, GetFileSystem())};
}

FSResult<std::shared_ptr<FileWriter>> LinkDirectory::DoCreateFileWriter(const std::string& filePath,
                                                                        const WriterOption& writerOption) noexcept
{
    if (writerOption.openType == FSOT_SLICE) {
        return LocalDirectory::DoCreateFileWriter(filePath, writerOption);
    }
    AUTIL_LOG(ERROR, "LinkDirectory [%s] not support CreateFileWriter", GetRootDir().c_str());
    return {FSEC_NOTSUP, std::shared_ptr<FileWriter>()};
}
}} // namespace indexlib::file_system
