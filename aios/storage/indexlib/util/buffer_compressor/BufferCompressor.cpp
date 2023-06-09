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
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, BufferCompressor);

CompressHintData* BufferCompressor::CreateHintData(const autil::StringView& hintData, bool needCopy) noexcept
{
    if (hintData.empty()) {
        return nullptr;
    }

    CompressHintData* obj = CreateHintDataObject();
    if (obj == nullptr) {
        AUTIL_LOG(ERROR, "compressor [%s] not support create hint data object.", _compressorName.c_str());
        return nullptr;
    }

    if (!obj->Init(hintData, needCopy)) {
        DELETE_AND_SET_NULL(obj);
        AUTIL_LOG(ERROR, "compressor [%s] init compress hint data fail, data size [%lu].", _compressorName.c_str(),
                  hintData.size());
        return nullptr;
    }
    return obj;
}

}} // namespace indexlib::util
