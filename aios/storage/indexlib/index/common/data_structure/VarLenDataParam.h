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
#include <memory>

#include "indexlib/file_system/WriterOption.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlibv2::index {

struct VarLenDataParam {
    bool enableAdaptiveOffset = false; /* use adaptive offset */
    bool equalCompressOffset = false;  /* use equal compress for offset */
    bool dataItemUniqEncode = false;   /* use uniq encode for data item */
    bool appendDataItemLength = false; /* append length for single item */
    bool disableGuardOffset = false;   /* disable append last offset for data length */
    uint64_t offsetThreshold =
        indexlib::index::VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD; /* threshold for enableAdaptiveOffset = true */
    std::string dataCompressorName;                   /* empty will not compress, support zstd/lz4/snappy/zlib... */
    std::string compressWriterExcludePattern;         /* if match pattern, will not create compress file writer */
    uint64_t dataCompressBufferSize = 4096;           /* data default compress buffer 4K */
    indexlib::util::KeyValueMap dataCompressorParams; /* data compressor parameter */

    bool operator==(const VarLenDataParam& other) const
    {
        return enableAdaptiveOffset == other.enableAdaptiveOffset && equalCompressOffset == other.equalCompressOffset &&
               dataItemUniqEncode == other.dataItemUniqEncode && appendDataItemLength == other.appendDataItemLength &&
               disableGuardOffset == other.disableGuardOffset && offsetThreshold == other.offsetThreshold &&
               dataCompressorName == other.dataCompressorName &&
               dataCompressBufferSize == other.dataCompressBufferSize &&
               compressWriterExcludePattern == other.compressWriterExcludePattern &&
               dataCompressorParams == other.dataCompressorParams;
    }

    void SyncCompressParam(indexlib::file_system::WriterOption& option) const
    {
        option.compressorName = dataCompressorName;
        option.compressBufferSize = dataCompressBufferSize;
        option.compressExcludePattern = compressWriterExcludePattern;
        option.compressorParams = dataCompressorParams;
    }
};
} // namespace indexlibv2::index
