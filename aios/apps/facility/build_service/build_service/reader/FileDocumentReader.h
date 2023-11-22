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
#include "build_service/reader/FileReaderBase.h"
#include "build_service/reader/Separator.h"
#include "build_service/util/Log.h"
#include "fslib/fslib.h"

namespace build_service { namespace reader {

class FileDocumentReader
{
public:
    FileDocumentReader(uint32_t bufferSize);
    virtual ~FileDocumentReader();

private:
    FileDocumentReader(const FileDocumentReader&);
    FileDocumentReader& operator=(const FileDocumentReader&);

public:
    virtual bool init(const std::string& fileName, int64_t offset);
    virtual bool read(std::string& docStr);

    virtual int64_t getFileOffset() const { return _fileOffset; }

    virtual bool isEof() const { return _isEof; }

    uint64_t getReadDocCount() const { return _readDocCount; }

    int64_t getFileSize() const { return _fReader ? _fReader->getFileLength() : 0; }

    int64_t getReadBytes() const { return _fReader ? _fReader->getReadBytes() : 0; }

    bool seek(int64_t offset);

protected:
    virtual bool doRead(std::string& docStr) = 0;
    virtual FileReaderBase* createInnerFileReader(const std::string& fileName);

    bool fillBuffer();
    void readUntilPos(char* pos, uint32_t sepLen, std::string& docStr);
    void readUntilPos(char* pos, uint32_t sepLen, char*& buffer);

    bool read(char* buffer, size_t len);

    inline bool readVUInt32(uint32_t& ret)
    {
        uint8_t byte;
        if (!read((char*)&byte, sizeof(uint8_t))) {
            return false;
        }
        uint32_t value = byte & 0x7F;
        int shift = 7;
        while (byte & 0x80) {
            if (!read((char*)&byte, sizeof(uint8_t))) {
                return false;
            }
            value |= ((byte & 0x7F) << shift);
            shift += 7;
        }
        ret = value;
        return true;
    }

protected:
    char* _bufferCursor;
    char* _bufferEnd;
    char* _buffer;
    uint32_t _bufferSize;
    int64_t _fileOffset;        // locator
    FileReaderBasePtr _fReader; // file
    std::string _fileName;
    bool _isEof;
    bool _needReinit;
    uint64_t _readDocCount;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FileDocumentReader);

}} // namespace build_service::reader
