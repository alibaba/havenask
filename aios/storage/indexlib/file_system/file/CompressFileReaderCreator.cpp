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
#include "indexlib/file_system/file/CompressFileReaderCreator.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/DecompressCachedCompressFileReader.h"
#include "indexlib/file_system/file/IntegratedCompressFileReader.h"
#include "indexlib/file_system/file/NormalCompressFileReader.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/cache/BlockAccessCounter.h"
#include "indexlib/util/cache/BlockCache.h"

namespace indexlib { namespace file_system {
class Directory;
}} // namespace indexlib::file_system

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, CompressFileReaderCreator);

CompressFileReaderCreator::CompressFileReaderCreator() {}

CompressFileReaderCreator::~CompressFileReaderCreator() {}

FSResult<CompressFileReaderPtr> CompressFileReaderCreator::Create(const std::shared_ptr<FileReader>& fileReader,
                                                                  const std::shared_ptr<FileReader>& metaReader,
                                                                  const std::shared_ptr<CompressFileInfo>& compressInfo,
                                                                  IDirectory* directory) noexcept
{
    assert(fileReader);

    CompressFileReaderPtr compressReader;
    if (fileReader->GetBaseAddress()) {
        compressReader.reset(new IntegratedCompressFileReader);
    } else if (NeedCacheDecompressFile(fileReader, compressInfo)) {
        compressReader.reset(new DecompressCachedCompressFileReader);
    } else {
        compressReader.reset(new NormalCompressFileReader);
    }

    try {
        if (!compressReader->Init(fileReader, metaReader, compressInfo, directory)) {
            AUTIL_LOG(ERROR, "Init compress file reader for [%s] fail!", fileReader->DebugString().c_str());
            return {FSEC_ERROR, CompressFileReaderPtr()};
        }
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "Init compress file reader for [%s] fail! exception[%s]", fileReader->DebugString().c_str(),
                  e.what());
        return {FSEC_ERROR, CompressFileReaderPtr()};
    } catch (...) {
        AUTIL_LOG(ERROR, "Init compress file reader for [%s] fail!", fileReader->DebugString().c_str());
        return {FSEC_ERROR, CompressFileReaderPtr()};
    }

    compressReader->InitDecompressMetricReporter(directory->GetFileSystem()->GetFileSystemMetricsReporter());
    return {FSEC_OK, compressReader};
}

bool CompressFileReaderCreator::NeedCacheDecompressFile(const std::shared_ptr<FileReader>& fileReader,
                                                        const std::shared_ptr<CompressFileInfo>& compressInfo) noexcept
{
    std::shared_ptr<BlockFileNode> blockFileNode = std::dynamic_pointer_cast<BlockFileNode>(fileReader->GetFileNode());
    if (!blockFileNode) {
        return false;
    }

    if (!blockFileNode->EnableCacheDecompressFile()) {
        return false;
    }

    BlockCache* blockCache = blockFileNode->GetBlockCache();
    assert(blockCache);
    if ((blockCache->GetBlockSize() % compressInfo->blockSize) != 0) {
        AUTIL_LOG(WARN,
                  "blockSize mismatch: compress block size [%lu], "
                  "blockCache block size [%lu], will not cache decompress file!",
                  compressInfo->blockSize, blockCache->GetBlockSize());
        return false;
    }
    return true;
}

}} // namespace indexlib::file_system
