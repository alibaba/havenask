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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class FileReaderBase
{
public:
    enum FileType {
        FILE_TYPE = 0,
        GZIP_TYPE = 1,
    };

public:
    FileReaderBase() : _fileTotalSize(0) {}
    virtual ~FileReaderBase() {}

private:
    FileReaderBase(const FileReaderBase&);
    FileReaderBase& operator=(const FileReaderBase&);

public:
    static FileReaderBase* createFileReader(const std::string& fileName, uint32_t bufferSize);
    static FileType getFileType(const std::string& fileName);

public:
    virtual bool init(const std::string& fileName, int64_t offset) = 0;
    virtual bool get(char* output, uint32_t size, uint32_t& sizeUsed) = 0;
    virtual bool good() = 0;
    virtual bool isEof() = 0;
    virtual int64_t getReadBytes() const = 0;
    virtual bool seek(int64_t offset) = 0;
    int64_t getFileLength() const { return _fileTotalSize; };

protected:
    int64_t _fileTotalSize;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FileReaderBase);

}} // namespace build_service::reader
