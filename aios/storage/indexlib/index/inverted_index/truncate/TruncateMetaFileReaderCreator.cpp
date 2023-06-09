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
#include "indexlib/index/inverted_index/truncate/TruncateMetaFileReaderCreator.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/FileReader.h"

namespace indexlib::index {
std::pair<Status, std::shared_ptr<file_system::FileReader>>
TruncateMetaFileReaderCreator::Create(const std::string& fileName, const file_system::ReaderOption& readerOption)
{
    assert(_truncateMetaDir != nullptr);
    auto fsResult = _truncateMetaDir->CreateFileReader(fileName, readerOption);
    if (!fsResult.OK()) {
        if (fsResult.Code() != indexlib::file_system::ErrorCode::FSEC_NOENT) {
            return {Status::InternalError(), nullptr};
        }
        return {Status::OK(), nullptr};
    }
    return fsResult.StatusWith();
}
} // namespace indexlib::index
