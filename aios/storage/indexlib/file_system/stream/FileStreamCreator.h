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

#include "autil/Log.h"

namespace autil::mem_pool {
class Pool;
}
namespace indexlib::file_system {

class FileStream;
class FileReader;
class FileStreamCreator
{
public:
    static std::shared_ptr<FileStream> CreateFileStream(const std::shared_ptr<file_system::FileReader>& fileReader,
                                                        autil::mem_pool::Pool* pool);

    // create file stream, support concurrency read, maybe more overhead
    static std::shared_ptr<FileStream>
    CreateConcurrencyFileStream(const std::shared_ptr<file_system::FileReader>& fileReader,
                                autil::mem_pool::Pool* pool);

private:
    static std::shared_ptr<FileStream> InnerCreateFileStream(const std::shared_ptr<file_system::FileReader>& fileReader,
                                                             autil::mem_pool::Pool* pool, bool supportConcurrency);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::file_system
