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

#include <stdint.h>

#include "indexlib/file_system/FileSystemDefine.h"

namespace indexlib { namespace file_system {

struct ReaderOption {
    enum CacheMode {
        CM_DEFAULT = 0, // default
        CM_USE = 1,     // will put file node into cache, even if file system useCache=false
        CM_SKIP = 2,    // will by pass cache and create a new file node
    };
    static const uint32_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;

    std::string linkRoot;               // inner parameter, for logical file system
    int64_t fileOffset = -1;            // inner parameter, for logical file system
    int64_t fileLength = -1;            // inner parameter, for logical file system
    FSOpenType openType = FSOT_UNKNOWN; // open type
    FSFileType fileType = FSFT_UNKNOWN; // file type
    CacheMode cacheMode = CM_DEFAULT;   // enum CacheMode
    bool isSwapMmapFile = false;        // if true, file node will open with swap mmap mode
    bool isWritable = false;            // if true, file node will be modify by address access
    bool supportCompress = false;       // if true, support create compress file reader when file is compressed
    bool cacheFirst =
        false; // when file node exists in cache: if true, will return from cache; if false will return with openType.
    bool mayNonExist = false;     // if true, will not print error log when file not exist
    bool forceRemotePath = false; // if true, will use remote path by force

    ReaderOption(FSOpenType openType_) : openType(openType_) {}; // TODO: mark explicit
    bool ForceSkipCache() const { return cacheMode == CM_SKIP; }
    bool ForceUseCache() const { return cacheMode == CM_USE; }

    static ReaderOption SwapMmap()
    {
        ReaderOption readerOption(FSOT_MMAP);
        readerOption.isSwapMmapFile = true;
        return readerOption;
    }

    static ReaderOption SupportCompress(FSOpenType openType)
    {
        ReaderOption readerOption(openType);
        readerOption.supportCompress = true;
        return readerOption;
    }

    static ReaderOption Writable(FSOpenType openType)
    {
        ReaderOption readerOption(openType);
        readerOption.isWritable = true;
        return readerOption;
    }
    static ReaderOption NoCache(FSOpenType openType)
    {
        ReaderOption readerOption(openType);
        readerOption.cacheMode = CM_SKIP;
        return readerOption;
    }
    static ReaderOption PutIntoCache(FSOpenType openType)
    {
        ReaderOption readerOption(openType);
        readerOption.cacheMode = CM_USE;
        return readerOption;
    }
    static ReaderOption CacheFirst(FSOpenType openType)
    {
        ReaderOption readerOption(openType);
        readerOption.cacheFirst = true;
        return readerOption;
    }
};
}} // namespace indexlib::file_system
