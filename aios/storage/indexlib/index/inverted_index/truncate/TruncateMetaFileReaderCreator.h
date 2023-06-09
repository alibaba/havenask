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

#include "indexlib/base/Status.h"

namespace indexlib::file_system {
class FileReader;
class IDirectory;
struct ReaderOption;
} // namespace indexlib::file_system

namespace indexlib::index {
class TruncateMetaFileReaderCreator
{
public:
    TruncateMetaFileReaderCreator(const std::shared_ptr<indexlib::file_system::IDirectory>& dir) : _truncateMetaDir(dir)
    {
    }
    ~TruncateMetaFileReaderCreator() = default;

    std::pair<Status, std::shared_ptr<indexlib::file_system::FileReader>>
    Create(const std::string& fileName, const indexlib::file_system::ReaderOption& readerOption);

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _truncateMetaDir;
};
} // namespace indexlib::index
