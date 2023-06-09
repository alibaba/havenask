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
#include "indexlib/file_system/relocatable/RelocatableFolder.h"

#include "indexlib/base/Status.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, RelocatableFolder);

std::pair<Status, std::shared_ptr<FileWriter>> RelocatableFolder::CreateFileWriter(const std::string& fileName,
                                                                                   const WriterOption& writerOption)
{
    assert(_folderRoot);
    return _folderRoot->CreateFileWriter(fileName, writerOption).StatusWith();
}

std::shared_ptr<RelocatableFolder> RelocatableFolder::MakeRelocatableFolder(const std::string& folderName)
{
    auto [status, dir] = _folderRoot->MakeDirectory(folderName, DirectoryOption()).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "make directory[%s] failed", folderName.c_str());
        return nullptr;
    }
    return std::make_shared<RelocatableFolder>(dir);
}

Status RelocatableFolder::RemoveFile(const std::string& fileName, const RemoveOption& removeOption)
{
    assert(_folderRoot);
    return _folderRoot->RemoveFile(fileName, removeOption).Status();
}
} // namespace indexlib::file_system
