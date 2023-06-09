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
#include "build_service/reader/AESCipherFileReader.h"

#include "autil/cipher/AESCipherCreator.h"
#include "build_service/reader/FileReader.h"

using namespace std;
using namespace fslib;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, AESCipherFileReader);

AESCipherFileReader::AESCipherFileReader(autil::cipher::CipherOption option) : _hasError(false)
{
    _decrypter = autil::cipher::AESCipherCreator::createStreamDecrypter(option, 2 * 1024 * 1024 /* 2MB */);
}

AESCipherFileReader::~AESCipherFileReader()
{
    if (_readThread) {
        _readThread->join();
    }
    if (_innerFile) {
        delete _innerFile;
    }
}

bool AESCipherFileReader::init(const std::string& fileName, int64_t offset)
{
    if (_decrypter == nullptr) {
        BS_LOG(ERROR, "get null stream decrypter!");
        return false;
    }
    _innerFile = createInnerFileReader();
    if (_innerFile == nullptr) {
        BS_LOG(ERROR, "failed to new inner file reader");
        return false;
    }
    if (!_innerFile->init(fileName, 0)) {
        return false;
    }

    _fileTotalSize = _innerFile->getFileLength();
    if (_fileTotalSize == 0) {
        string errorMsg = "aes cipher file[" + fileName + "] size == 0. ";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        _decrypter->seal();
        return true;
    }

    auto asyncRead = [this]() {
        uint32_t bufferLen = 1024 * 1024; /* 1MB */
        vector<char> buffer;
        buffer.resize(bufferLen);
        while (!_hasError) {
            uint32_t sizeUsed = 0;
            if (!_innerFile->get((char*)buffer.data(), bufferLen, sizeUsed)) {
                if (_innerFile->isEof()) {
                    break;
                }
                _hasError = true;
                continue;
            }
            if (sizeUsed == 0) {
                continue;
            }
            if (!_decrypter->append((const unsigned char*)buffer.data(), sizeUsed)) {
                AUTIL_LOG(ERROR, "decrypter append data fail.");
                _hasError = true;
                continue;
            }
        }
        if (!_hasError && !_decrypter->seal()) {
            AUTIL_LOG(ERROR, "decrypter seal fail.");
            _hasError = true;
        }
    };
    _readThread.reset(new std::thread(asyncRead));
    if (offset != 0 && !skipToOffset(offset)) {
        BS_LOG(ERROR, "failed to skip to offset [%ld]", offset);
        return false;
    }
    return true;
}

bool AESCipherFileReader::get(char* output, uint32_t size, uint32_t& sizeUsed)
{
    if (_decrypter == nullptr) {
        return false;
    }
    sizeUsed = 0;
    while (sizeUsed < size) {
        uint32_t sizeToRead = size - sizeUsed;
        char* buffer = output + sizeUsed;
        uint32_t sizeRead = 0;
        if (!innerGet(buffer, sizeToRead, sizeRead)) {
            return sizeUsed > 0;
        }
        sizeUsed += sizeRead;
    }
    return true;
}

bool AESCipherFileReader::innerGet(char* output, uint32_t size, uint32_t& sizeUsed)
{
    assert(_decrypter != nullptr);
    while (!_hasError) {
        auto ret = _decrypter->get((unsigned char*)output, size);
        if (ret > 0) {
            sizeUsed = ret;
            return true;
        }
        if (_decrypter->isEof()) {
            return false; // reach eof
        }
    }
    return !_hasError;
}

bool AESCipherFileReader::good()
{
    assert(_innerFile && _decrypter);
    return _innerFile->good();
}

bool AESCipherFileReader::isEof()
{
    assert(_innerFile && _decrypter);
    return _innerFile->isEof() && _decrypter->isEof();
}

int64_t AESCipherFileReader::getReadBytes() const
{
    assert(_innerFile && _decrypter);
    return _innerFile->getReadBytes();
}

bool AESCipherFileReader::seek(int64_t offset)
{
    BS_LOG(ERROR, "AESCipherFileReader do not support seek");
    return false;
}

bool AESCipherFileReader::skipToOffset(int64_t offset)
{
    int64_t totalRead = 0;
    uint32_t sizeRead = 0;
    const uint32_t TEMP_BUFFER_SIZE = 1024 * 1024 * 10; // 10 M
    char* tempBuff = new char[TEMP_BUFFER_SIZE];
    while (totalRead < offset) {
        uint32_t sizeToRead = (offset - totalRead) > TEMP_BUFFER_SIZE ? TEMP_BUFFER_SIZE : (offset - totalRead);
        if (!get(tempBuff, sizeToRead, sizeRead)) {
            BS_LOG(ERROR, "failed to read data for length of [%u]", sizeToRead);
            delete[] tempBuff;
            return false;
        }
        totalRead += sizeRead;
    }
    delete[] tempBuff;
    return true;
}

FileReaderBase* AESCipherFileReader::createInnerFileReader() { return new (nothrow) FileReader; }

}} // namespace build_service::reader
