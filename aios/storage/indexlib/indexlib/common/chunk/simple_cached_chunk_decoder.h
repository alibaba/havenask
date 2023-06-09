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
#ifndef __INDEXLIB_SIMPLE_CACHED_CHUNK_DECODER_H
#define __INDEXLIB_SIMPLE_CACHED_CHUNK_DECODER_H

#include <memory>
#include <unordered_map>

#include "autil/ConstString.h"
#include "autil/Lock.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common/chunk/chunk_define.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

class SimpleCachedChunkDecoder
{
public:
    SimpleCachedChunkDecoder();
    ~SimpleCachedChunkDecoder();

public:
    void Read(file_system::FileReader* fileReader, uint64_t offset, autil::mem_pool::Pool* pool, ChunkMeta& meta,
              autil::StringView& data);

    void Reset();

private:
    struct ChunkInfo {
        ChunkInfo() : chunkOffset(0), chunkData(autil::StringView::empty_instance()) {}

        ~ChunkInfo()
        {
            mutex.lock();
            if (chunkData.data() != NULL) {
                char* data = (char*)chunkData.data();
                delete[] data;
                chunkData = autil::StringView::empty_instance();
            }
            mutex.unlock();
        }
        uint64_t chunkOffset;
        ChunkMeta chunkMeta;
        autil::StringView chunkData;
        autil::ThreadMutex mutex;
    };

private:
    typedef std::unordered_map<std::string, ChunkInfo*> ChunkInfoMap;
    ChunkInfoMap mChunkMap;
    autil::ReadWriteLock mRWLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SimpleCachedChunkDecoder);
}} // namespace indexlib::common

#endif //__INDEXLIB_SIMPLE_CACHED_CHUNK_DECODER_H
