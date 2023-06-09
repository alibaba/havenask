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
#include "indexlib/util/buffer_compressor/Lz4Compressor.h"

#include "indexlib/util/buffer_compressor/Lz4CompressHintData.h"
#include "indexlib/util/buffer_compressor/ZstdHintDataTrainer.h"

using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, Lz4Compressor);

const std::string Lz4Compressor::COMPRESSOR_NAME = "lz4";
const std::string Lz4Compressor::COMPRESSOR_LIB_VERSION = LZ4_VERSION_STRING;

CompressHintDataTrainer* Lz4Compressor::CreateTrainer(size_t maxBlockSize, size_t trainBlockCount,
                                                      size_t hintDataCapacity, float sampleRatio) const noexcept
{
    // lz4 reuse zstd hint dictionary data
    CompressHintDataTrainer* obj = new ZstdHintDataTrainer();
    if (obj->Init(maxBlockSize, trainBlockCount, hintDataCapacity, sampleRatio)) {
        return obj;
    }
    AUTIL_LOG(ERROR, "init zstd hit data trainer fail.");
    delete obj;
    return nullptr;
}

CompressHintData* Lz4Compressor::CreateHintDataObject() const noexcept { return new Lz4CompressHintData(); }

}} // namespace indexlib::util
