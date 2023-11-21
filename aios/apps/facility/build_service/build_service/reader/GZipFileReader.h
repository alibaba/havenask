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

#include <zlib.h>

#include "build_service/common_define.h"
#include "build_service/reader/FileReaderBase.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class GZipFileReader : public FileReaderBase
{
public:
    GZipFileReader(uint32_t bufferSize, FileReaderBase* next = NULL);
    ~GZipFileReader();

private:
    GZipFileReader(const GZipFileReader&);
    GZipFileReader& operator=(const GZipFileReader&);

public:
    bool init(const std::string& fileName, int64_t offset) override;
    bool get(char* output, uint32_t size, uint32_t& sizeUsed) override;

    bool good() override { return _next->good(); }
    bool isEof() override { return _next->isEof() && _bufferNow >= _bufferEnd; }
    int64_t getReadBytes() const override { return _next == NULL ? 0 : _next->getReadBytes(); }
    bool seek(int64_t offset) override;

private:
    bool reloadBuffer();
    bool readGzipHeader();
    bool skipToOffset(int64_t offset);

protected:
    virtual FileReaderBase* createInnerFileReader();

private:
    bool _ownNextReader;
    FileReaderBase* _next;
    uint32_t _bufferSize;
    char* _buffer;
    uint32_t _bufferNow;
    uint32_t _bufferEnd;

    z_stream _strm;

private:
    static const uint32_t TEMP_BUFFER_SIZE = 1024 * 1024 * 10; // 10 M

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GZipFileReader);

}} // namespace build_service::reader
