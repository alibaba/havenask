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
#include "indexlib/util/buffer_compressor/ZstdCompressHintData.h"

using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, ZstdCompressHintData);

ZstdCompressHintData::ZstdCompressHintData() : _dDict(nullptr) {}

ZstdCompressHintData::~ZstdCompressHintData()
{
    if (_dDict) {
        ZSTD_freeDDict(_dDict);
        _dDict = nullptr;
    }
}

bool ZstdCompressHintData::Init(const autil::StringView& data, bool needCopy)
{
    if (data.empty()) {
        return false;
    }
    assert(!_dDict);
    if (needCopy) {
        _dDict = ZSTD_createDDict((const void*)data.data(), data.size());
    } else {
        _dDict = ZSTD_createDDict_byReference((const void*)data.data(), data.size());
    }
    return _dDict != nullptr;
}

}} // namespace indexlib::util
