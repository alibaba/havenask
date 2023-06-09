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
#include "indexlib/file_system/stream/FileStreamCreator.h"

#include "autil/memory.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/stream/BlockFileStream.h"
#include "indexlib/file_system/stream/CompressFileStream.h"
#include "indexlib/file_system/stream/NormalFileStream.h"

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileStreamCreator);

std::shared_ptr<FileStream>
FileStreamCreator::CreateFileStream(const std::shared_ptr<file_system::FileReader>& fileReader,
                                    autil::mem_pool::Pool* pool)
{
    return InnerCreateFileStream(fileReader, pool, false);
}

std::shared_ptr<FileStream>
FileStreamCreator::CreateConcurrencyFileStream(const std::shared_ptr<file_system::FileReader>& fileReader,
                                               autil::mem_pool::Pool* pool)
{
    return InnerCreateFileStream(fileReader, pool, true);
}

std::shared_ptr<FileStream>
FileStreamCreator::InnerCreateFileStream(const std::shared_ptr<file_system::FileReader>& fileReader,
                                         autil::mem_pool::Pool* pool, bool supportConcurrency)
{
    if (supportConcurrency && std::dynamic_pointer_cast<file_system::BufferedFileReader>(fileReader)) {
        AUTIL_LOG(ERROR, "buffer file reader not supoort concurrency, file [%s]", fileReader->DebugString().c_str());
        return nullptr;
    }

    if (auto compressFileReader = std::dynamic_pointer_cast<file_system::CompressFileReader>(fileReader)) {
        return autil::shared_ptr_pool(
            pool, IE_POOL_COMPATIBLE_NEW_CLASS(pool, CompressFileStream, compressFileReader, supportConcurrency, pool));
    }

    if (auto blockFileNode = std::dynamic_pointer_cast<file_system::BlockFileNode>(fileReader->GetFileNode())) {
        return autil::shared_ptr_pool(
            pool, IE_POOL_COMPATIBLE_NEW_CLASS(pool, BlockFileStream, fileReader, supportConcurrency));
    }

    return autil::shared_ptr_pool(pool,
                                  IE_POOL_COMPATIBLE_NEW_CLASS(pool, NormalFileStream, fileReader, supportConcurrency));
}

} // namespace indexlib::file_system
