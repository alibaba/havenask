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

#include "indexlib/index/kkv/built/KKVBuiltValueReader.h"
#include "indexlib/index/kkv/common/KKVDocs.h"

namespace indexlibv2::index {

class KKVBuiltValueFetcher
{
private:
    using DocIter = typename KKVDocs::iterator;

public:
    void Init(KKVBuiltValueReader* reader) { _reader = reader; }
    operator bool() const { return _reader != nullptr; }
    FL_LAZY(bool) FetchValues(DocIter begin, DocIter end)
    {
        assert(_reader);
        for (auto iter = begin; iter != end; ++iter) {
            auto& doc = *iter;
            auto valueOffset = doc.valueOffset;
            if (_reader->NeedSwitchChunk(valueOffset.chunkOffset)) {
                if (!(FL_COAWAIT _reader->SwitchChunk(valueOffset.chunkOffset))) {
                    FL_CORETURN false;
                }
            }
            doc.value = _reader->ReadInChunk(valueOffset.inChunkOffset);
        }
        FL_CORETURN true;
    }

private:
    KKVBuiltValueReader* _reader = nullptr;
};

} // namespace indexlibv2::index
