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
#include "build_service/reader/FileDocumentReader.h"

#include "build_service/reader/FileReader.h"
#include "build_service/reader/GZipFileReader.h"

using namespace std;
namespace build_service { namespace reader {
BS_LOG_SETUP(reader, FileDocumentReader);

FileDocumentReader::FileDocumentReader(uint32_t bufferSize)
    : _bufferCursor(NULL)
    , _bufferEnd(NULL)
    , _buffer(NULL)
    , _bufferSize(bufferSize)
    , _fileOffset(0)
    , _isEof(true)
    , _needReinit(true)
    , _readDocCount(0)
{
}

FileDocumentReader::~FileDocumentReader()
{
    if (!_buffer) {
        return;
    }
    delete[] _buffer;
    _buffer = NULL;
}

bool FileDocumentReader::read(string& docStr)
{
    if (_needReinit && !init(_fileName, _fileOffset)) {
        return false;
    }
    int64_t previousOffset = _fileOffset;
    if (!doRead(docStr)) {
        if (!isEof()) {
            _fileOffset = previousOffset;
        }
        return false;
    }
    _readDocCount++;
    return true;
}

bool FileDocumentReader::init(const string& fileName, int64_t offset)
{
    _fileName = fileName;
    _fReader.reset(createInnerFileReader(fileName));
    assert(_fReader != NULL);
    if (!_fReader->init(fileName, offset)) {
        stringstream ss;
        ss << "failed to init freader with fileName[" << fileName << "], offset[" << offset << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (!_fReader->good()) {
        string errorMsg = "failed to init freader with fileName[" + fileName + "], open fail!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _fileOffset = offset;
    _isEof = false;
    _needReinit = false;
    if (_buffer) {
        delete[] _buffer;
    }
    _buffer = new char[_bufferSize];
    _bufferCursor = _buffer;
    _bufferEnd = _buffer;
    return true;
}

void FileDocumentReader::readUntilPos(char* pos, uint32_t sepLen, string& docStr)
{
    docStr.append(_bufferCursor, pos);
    char* nextCursor = pos + sepLen;
    _fileOffset += nextCursor - _bufferCursor;
    _bufferCursor = nextCursor;
}

void FileDocumentReader::readUntilPos(char* pos, uint32_t sepLen, char*& buffer)
{
    memcpy(buffer, _bufferCursor, pos - _bufferCursor);
    buffer += (pos - _bufferCursor);
    char* nextCursor = pos + sepLen;
    _fileOffset += nextCursor - _bufferCursor;
    _bufferCursor = nextCursor;
}

bool FileDocumentReader::read(char* buffer, size_t len)
{
    while (len > 0) {
        char* pos = _bufferCursor + len;
        if (pos <= _bufferEnd) {
            readUntilPos(pos, 0, buffer);
            return true;
        }

        pos = _bufferEnd;
        len -= (pos - _bufferCursor);
        readUntilPos(pos, 0, buffer);

        if (!fillBuffer()) {
            return false;
        }
    }
    return true;
}

bool FileDocumentReader::fillBuffer()
{
    int32_t moveMemSize = _bufferEnd - _bufferCursor;
    memmove(_buffer, _bufferCursor, moveMemSize);
    _bufferCursor = _buffer;
    _bufferEnd = _buffer + moveMemSize;

    uint32_t needRead = _bufferSize - moveMemSize;
    uint32_t sizeRead;
    if (_fReader->get(_bufferEnd, needRead, sizeRead)) {
        _bufferEnd += sizeRead;
        return true;
    } else if (_fReader->isEof()) {
        _isEof = true;
        return false;
    } else {
        _needReinit = true;
        return false;
    }
}

bool FileDocumentReader::seek(int64_t offset)
{
    if (!_fReader->seek(offset)) {
        return false;
    }
    _fileOffset = offset;
    _bufferCursor = _buffer;
    _bufferEnd = _buffer;
    return true;
}

FileReaderBase* FileDocumentReader::createInnerFileReader(const std::string& fileName)
{
    FileReaderBase::FileType fileType = FileReaderBase::getFileType(fileName);
    if (fileType == FileReaderBase::GZIP_TYPE) {
        return new GZipFileReader(_bufferSize);
    }
    return new FileReader;
}

}} // namespace build_service::reader
