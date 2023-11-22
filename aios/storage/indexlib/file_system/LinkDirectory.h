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
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/WriterOption.h"

namespace indexlib { namespace file_system {

// LinkDirectory is read only, for flushed rt segments
class LinkDirectory final : public LocalDirectory
{
public:
    LinkDirectory(const std::string& path, const std::shared_ptr<IFileSystem>& fileSystem);
    ~LinkDirectory();

public:
    FSResult<std::shared_ptr<IDirectory>> MakeDirectory(const std::string& dirPath,
                                                        const DirectoryOption& directoryOption) noexcept override;
    FSResult<std::shared_ptr<IDirectory>> GetDirectory(const std::string& dirPath) noexcept override;

private:
    FSResult<std::shared_ptr<FileWriter>> DoCreateFileWriter(const std::string& filePath,
                                                             const WriterOption& writerOption) noexcept final override;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::file_system
