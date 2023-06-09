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

#include "autil/legacy/jsonizable.h"

namespace worker_framework {

class DataOption : public autil::legacy::Jsonizable {
public:
    DataOption(bool needExpand) {
        DataOption();
        this->needExpand = needExpand;
    }

    DataOption()
        : retryCountLimit(0)
        , blockSize(16 * 1024 * 1024)
        , checkData(false)
        , useDirectIo(true)
        , pageCacheLimit(-1)
        , syncSizeLimit(4 * 1024 * 1024)
        , syncInterval(0)
        , openFileTimeout(60 * 1000)
        , needExpand(true)
        , isCheckExist(true) {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("retry_count_limit", retryCountLimit, retryCountLimit);
        json.Jsonize("block_size", blockSize, blockSize);
        json.Jsonize("check_data", checkData, checkData);
        json.Jsonize("use_directio", useDirectIo, useDirectIo);
        json.Jsonize("pagecache_limit", pageCacheLimit, pageCacheLimit);
        json.Jsonize("sync_size_limit", syncSizeLimit, syncSizeLimit);
        json.Jsonize("sync_interval", syncInterval, syncInterval);
        json.Jsonize("open_file_timeout", openFileTimeout, openFileTimeout);
    }

public:
    int32_t retryCountLimit;
    int32_t blockSize;
    bool checkData;
    bool useDirectIo;
    int64_t pageCacheLimit;
    int64_t syncSizeLimit;
    int64_t syncInterval;
    int64_t openFileTimeout;
    bool needExpand;
    bool isCheckExist;
};

}; // namespace worker_framework
