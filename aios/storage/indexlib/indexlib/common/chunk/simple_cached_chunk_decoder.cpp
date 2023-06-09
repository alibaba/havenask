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
#include "indexlib/common/chunk/simple_cached_chunk_decoder.h"

#include <pthread.h>

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::file_system;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, SimpleCachedChunkDecoder);

SimpleCachedChunkDecoder::SimpleCachedChunkDecoder() {}

SimpleCachedChunkDecoder::~SimpleCachedChunkDecoder() { Reset(); }

void SimpleCachedChunkDecoder::Read(FileReader* fileReader, uint64_t offset, Pool* pool, ChunkMeta& meta,
                                    autil::StringView& data)
{
    assert(fileReader && fileReader->GetOpenType() == FSOT_BUFFERED);
    pthread_t tid = pthread_self();
    // TODO(qingran): join physical & logical path to support package file, maybe we need a specific interface for outer
    // cache
    string cacheKey =
        fileReader->GetPhysicalPath() + "#" + fileReader->GetLogicalPath() + "@" + StringUtil::toString(tid);

    mRWLock.rdlock();
    ChunkInfoMap::iterator iter = mChunkMap.find(cacheKey);
    ChunkInfo* info = (iter != mChunkMap.end()) ? iter->second : NULL;
    mRWLock.unlock();
    if (info == NULL) {
        fileReader->Read(&meta, sizeof(meta), offset).GetOrThrow();
        char* chunkData = (char*)pool->allocate(meta.length);
        fileReader->Read(chunkData, meta.length, offset + sizeof(meta)).GetOrThrow();

        char* dataBuf = new char[meta.length];
        memcpy(dataBuf, chunkData, meta.length);
        unique_ptr<ChunkInfo> info(new ChunkInfo);
        info->chunkData = StringView(dataBuf, meta.length);
        info->chunkOffset = offset;
        info->chunkMeta = meta;

        mRWLock.wrlock();
        mChunkMap.insert(make_pair(cacheKey, info.release()));
        mRWLock.unlock();
        data = StringView(chunkData, meta.length);
        return;
    }

    autil::ScopedLock lock(info->mutex);
    if (offset == info->chunkOffset) {
        // cache hit
        meta = info->chunkMeta;
        StringView cacheData = info->chunkData;
        char* chunkData = (char*)pool->allocate(meta.length);
        memcpy(chunkData, cacheData.data(), meta.length);
        data = StringView(chunkData, meta.length);
        return;
    }

    if (offset < info->chunkOffset) {
        IE_LOG(ERROR, "read backward last offset[%lu], current offset [%lu]", info->chunkOffset, offset);
    }

    fileReader->Read(&meta, sizeof(meta), offset).GetOrThrow();
    char* chunkData = (char*)pool->allocate(meta.length);
    fileReader->Read(chunkData, meta.length, offset + sizeof(meta)).GetOrThrow();

    char* dataBuf = (char*)info->chunkData.data();
    uint32_t dataBufLen = info->chunkData.size();
    if (dataBufLen < meta.length) {
        delete[] dataBuf;
        dataBuf = new char[meta.length];
        dataBufLen = meta.length;
    }

    memcpy(dataBuf, chunkData, meta.length);
    info->chunkData = StringView(dataBuf, dataBufLen);
    info->chunkOffset = offset;
    info->chunkMeta = meta;
    data = StringView(chunkData, meta.length);
}

void SimpleCachedChunkDecoder::Reset()
{
    mRWLock.wrlock();
    ChunkInfoMap::iterator iter = mChunkMap.begin();
    for (; iter != mChunkMap.end(); iter++) {
        if (iter->second != NULL) {
            delete iter->second;
            iter->second = NULL;
        }
    }
    mChunkMap.clear();
    mRWLock.unlock();
}
}} // namespace indexlib::common
