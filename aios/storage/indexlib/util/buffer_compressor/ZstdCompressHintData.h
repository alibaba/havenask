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

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include "autil/Log.h"
#include "indexlib/util/buffer_compressor/CompressHintData.h"

namespace indexlib { namespace util {

class ZstdCompressHintData : public CompressHintData
{
public:
    ZstdCompressHintData();
    ~ZstdCompressHintData();

    ZstdCompressHintData(const ZstdCompressHintData&) = delete;
    ZstdCompressHintData& operator=(const ZstdCompressHintData&) = delete;
    ZstdCompressHintData(ZstdCompressHintData&&) = delete;
    ZstdCompressHintData& operator=(ZstdCompressHintData&&) = delete;

public:
    bool Init(const autil::StringView& data, bool needCopy) override;
    ZSTD_DDict* GetDDict() const { return _dDict; }

private:
    ZSTD_DDict* _dDict;
    autil::StringView _data;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ZstdCompressHintData> ZstdCompressHintDataPtr;

}} // namespace indexlib::util
