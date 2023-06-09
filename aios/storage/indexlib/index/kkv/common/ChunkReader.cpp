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
#include "indexlib/index/kkv/common/ChunkReader.h"

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, ChunkReader);

FL_LAZY(ChunkData) ChunkReader::ReadAsync(uint64_t chunkOffset)
{
    if (!_isOnline) {
        // TODO(richard.sy), use cache read for offline merge
        AUTIL_INTERVAL_LOG(10000, WARN, "need optimize offline chunk read");
    }

    ChunkMeta meta;
    auto ec = FL_COAWAIT _fileReader->ReadAsyncCoro(&meta, sizeof(meta), chunkOffset, _readOption);
    if (!ec.OK() || ec.Value() != sizeof(meta)) {
        AUTIL_LOG(ERROR, "read len[%d] in[%s] failed, error[%s]", meta.length, _fileReader->DebugString().c_str(),
                  ec.Status().ToString().c_str());
        FL_CORETURN ChunkData::Invalid();
    }

    char* buffer = POOL_COMPATIBLE_NEW_VECTOR(_sessionPool, char, meta.length);
    ec = FL_COAWAIT _fileReader->ReadAsyncCoro(buffer, meta.length, chunkOffset + sizeof(meta), _readOption);
    if (!ec.OK() || ec.Value() != meta.length) {
        AUTIL_LOG(ERROR, "read len[%d] in[%s] failed, error[%s]", meta.length, _fileReader->DebugString().c_str(),
                  ec.Status().ToString().c_str());
        FL_CORETURN ChunkData::Invalid();
    }

    FL_CORETURN ChunkData::Of(buffer, meta.length);
}

} // namespace indexlibv2::index
