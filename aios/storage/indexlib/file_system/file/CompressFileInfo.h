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

#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib { namespace file_system {

class CompressFileInfo : public autil::legacy::Jsonizable
{
public:
    CompressFileInfo()
        : blockCount(0)
        , blockSize(0)
        , compressFileLen(0)
        , deCompressFileLen(0)
        , maxCompressBlockSize(0)
        , enableCompressAddressMapper(false)
    {
    }

    ~CompressFileInfo() {}

public:
    void FromString(const std::string& str) { FromJsonString(*this, str); }

    std::string ToString() const { return ToJsonString(*this); }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("compressor", compressorName, compressorName);
        json.Jsonize("block_count", blockCount, blockCount);
        json.Jsonize("block_size", blockSize, blockSize);
        json.Jsonize("compress_file_length", compressFileLen, compressFileLen);
        json.Jsonize("decompress_file_length", deCompressFileLen, deCompressFileLen);
        json.Jsonize("max_compress_block_size", maxCompressBlockSize, maxCompressBlockSize);
        json.Jsonize("enable_compress_address_mapper", enableCompressAddressMapper, enableCompressAddressMapper);
        json.Jsonize("additional_information", additionalInfo, additionalInfo);
    }

public:
    std::string compressorName;
    size_t blockCount;
    size_t blockSize;
    size_t compressFileLen;
    size_t deCompressFileLen;
    size_t maxCompressBlockSize;
    bool enableCompressAddressMapper;
    util::KeyValueMap additionalInfo;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CompressFileInfo> CompressFileInfoPtr;
}} // namespace indexlib::file_system
