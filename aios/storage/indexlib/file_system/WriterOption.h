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

typedef std::map<std::string, std::string> KeyValueMap;

struct WriterOption {
    static const uint32_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;
    static const uint32_t DEFAULT_COMPRESS_BUFFER_SIZE = 4 * 1024;

    int64_t fileLength = -1; // hint, -1 means unknown
    FSOpenType openType =
        FSOT_UNKNOWN; // support: FSOT_UNKNOWN(means auto), FSOT_BUFFERED, FSOT_MEM, FSOT_SLICE, FSOT_MMAP
    uint32_t bufferSize = DEFAULT_BUFFER_SIZE;
    int32_t sliceNum = -1;       // FSOT_SLICE only, >= 0 means FSOT_SLICE
    uint64_t sliceLen = 0;       // FSOT_SLICE only, valid when sliceNum >= 0
    bool noDump = false;         // if true, file will be a pure memory file
    bool atomicDump = false;     // if true, perform atomic execution when flush
    bool copyOnDump = false;     // if true, make cloned file node when flush
    bool asyncDump = false;      // if true, dump with other thread
    bool isSwapMmapFile = false; // if true, file node will create with swap mmap mode
    bool isAppendMode = false;   // only support BufferFileWriter
    bool notInPackage = false;   // if true, file will always be a stand alone file instead of may be in package
    std::string
        compressorName; // if empty, create uncompress file, otherwise create compress file according to compressorName
    uint32_t compressBufferSize =
        DEFAULT_COMPRESS_BUFFER_SIZE;   // when create compress file, set compress block buffer size
    std::string compressExcludePattern; // when target filePath match exclude file pattern, will not use file compress
    KeyValueMap compressorParams;
    FSMetricGroup metricGroup = FSMG_LOCAL;

    WriterOption() {}

    static WriterOption AtomicDump()
    {
        WriterOption writerOption;
        writerOption.atomicDump = true;
        return writerOption;
    }
    static WriterOption Buffer()
    {
        WriterOption writerOption;
        writerOption.openType = FSOT_BUFFERED;
        return writerOption;
    }
    static WriterOption BufferAtomicDump()
    {
        WriterOption writerOption;
        writerOption.openType = FSOT_BUFFERED;
        writerOption.atomicDump = true;
        return writerOption;
    }
    static WriterOption Mem(int64_t fileLength)
    {
        WriterOption writerOption;
        writerOption.openType = FSOT_MEM;
        writerOption.fileLength = fileLength;
        return writerOption;
    }
    static WriterOption MemNoDump(int64_t fileLength)
    {
        WriterOption writerOption;
        writerOption.openType = FSOT_MEM;
        writerOption.fileLength = fileLength;
        writerOption.noDump = true;
        return writerOption;
    }
    static WriterOption Slice(uint64_t sliceLen, int32_t sliceNum)
    { // need CreateFileReader before FileWriter::Close(), otherwise will lost entry
        WriterOption writerOption;
        writerOption.openType = FSOT_SLICE;
        writerOption.noDump = true;
        writerOption.sliceLen = sliceLen;
        writerOption.sliceNum = sliceNum;
        return writerOption;
    }
    static WriterOption SwapMmap(int64_t fileLength)
    {
        WriterOption writerOption;
        writerOption.openType = FSOT_MMAP;
        writerOption.isSwapMmapFile = true;
        writerOption.fileLength = fileLength;
        return writerOption;
    }
    static WriterOption Compress(const std::string& compressorName, size_t compressBuffSize,
                                 const std::string& excludePattern = "")
    {
        WriterOption writerOption;
        writerOption.compressorName = compressorName;
        writerOption.compressBufferSize = compressBuffSize;
        writerOption.compressExcludePattern = excludePattern;
        return writerOption;
    }
};
}} // namespace indexlib::file_system
