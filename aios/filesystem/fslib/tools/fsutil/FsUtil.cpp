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
#include "tools/fsutil/FsUtil.h"

#include <algorithm>
#include <thread>
#include <time.h>
#include <zlib.h>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/cipher/AESCipherCreator.h"
#include "autil/legacy/md5.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/SafeBuffer.h"

using namespace std;
FSLIB_USE_NAMESPACE(util);
FSLIB_USE_NAMESPACE(config);
FSLIB_USE_NAMESPACE(fs);
FSLIB_BEGIN_NAMESPACE(tools);

typedef autil::legacy::Md5Stream Md5Stream;
FSLIB_TYPEDEF_AUTO_PTR(Md5Stream);

string FsUtil::GETMETA_CMD = "getmeta";
string FsUtil::COPY_CMD = "cp";
string FsUtil::MD5_SUM_CMD = "md5sum";
string FsUtil::ENCRYPT_CMD = "encrypt";
string FsUtil::DECRYPT_CMD = "decrypt";
string FsUtil::DECRYPT_CAT_CMD = "dcat";
string FsUtil::DECRYPT_ZCAT_CMD = "dzcat";
string FsUtil::MOVE_CMD = "mv";
string FsUtil::MKDIR_CMD = "mkdir";
string FsUtil::LISTDIR_CMD = "ls";
string FsUtil::REMOVE_CMD = "rm";
string FsUtil::FLOCK_CMD = "flock";
string FsUtil::CAT_CMD = "cat";
string FsUtil::ZCAT_CMD = "zcat";
string FsUtil::ISEXIST_CMD = "isexist";
string FsUtil::RENAME_CMD = "rename";
string FsUtil::FORWARD_CMD = "forward";
const int32_t FsUtil::DEFAULT_THREAD_NUM = 128;

#pragma pack(push, 1)
struct GZipHeader {
    unsigned char magic1; // 31
    unsigned char magic2; // 139
    unsigned char method; // 8
    unsigned char flags;
    uint32_t modTime;
    unsigned char extraFlags;
    unsigned char os;
};
#pragma pack(pop)

template <typename T>
class GZipHeaderParser {
public:
    static bool parse(std::unique_ptr<T> &reader) {
        // read header off
        GZipHeader gzip_header;
        if (reader->read((char *)&gzip_header, sizeof(gzip_header)) != sizeof(gzip_header)) {
            cerr << "file header read failed" << endl;
            return false;
        }
        if ((gzip_header.magic1 != 31) || (gzip_header.magic2 != 139) || (gzip_header.method != Z_DEFLATED) ||
            (gzip_header.flags & 0xe0)) {
            cerr << "gzip file header check failed" << endl;
            return false;
        }
        if (gzip_header.flags & 4) { // extra field
            char buf[2];
            if (reader->read(buf, sizeof(buf)) != sizeof(buf)) {
                cerr << "extra field length read failed" << endl;
                return false;
            }
            unsigned int len = (unsigned)(buf[1]);
            len <<= 8;
            len += (unsigned)(buf[0]);
            while (len--) {
                char checkBuf[1];
                if (reader->read(checkBuf, sizeof(checkBuf)) != sizeof(checkBuf)) {
                    cerr << "extra field data read failed" << endl;
                    return false;
                }
                if (checkBuf[0] < 0) {
                    break;
                }
            }
        }
        if (gzip_header.flags & 8) { // file name
            char checkBuf[1];
            do {
                if (reader->read(checkBuf, sizeof(checkBuf)) != sizeof(checkBuf)) {
                    cerr << "file name read failed" << endl;
                    return false;
                }
            } while (checkBuf[0] > 0);
        }
        if (gzip_header.flags & 16) { // comment
            char checkBuf[1];
            do {
                if (reader->read(checkBuf, sizeof(checkBuf)) != sizeof(checkBuf)) {
                    cerr << "comment read failed" << endl;
                    return false;
                }
            } while (checkBuf[0] > 0);
        }
        if (gzip_header.flags & 2) { // header crc
            char checkBuf[2];
            if (reader->read(checkBuf, sizeof(checkBuf)) != sizeof(checkBuf)) {
                cerr << "header crc read failed" << endl;
                return false;
            }
        }
        return true;
    }
};

FsUtil::~FsUtil() {}

void FsUtil::timeToStr(uint64_t time, char *timestr) {
    time_t mytime = time;
    struct tm result;
    localtime_r(&mytime, &result);

    sprintf(timestr,
            "%d-%d-%d %d:%d:%d",
            result.tm_year + 1900,
            result.tm_mon + 1,
            result.tm_mday,
            result.tm_hour,
            result.tm_min,
            result.tm_sec);
}

ErrorCode FsUtil::runCopy(const char *srcPath, const char *dstPath, bool recursive) {
    if (strcmp(srcPath, dstPath) == 0) {
        cerr << "copy src <" << srcPath << "> to dst <" << dstPath << "> fail. cannot copy path to itself." << endl
             << endl
             << "operation fail! " << endl;
        return EC_NOTSUP;
    }

    AbstractFileSystem *srcFs;
    ErrorCode ret = FileSystem::parseInternal(srcPath, srcFs);
    if (ret != EC_OK) {
        cerr << "copy src <" << srcPath << "> to dst <" << dstPath << "> fail. parse src file fail. "
             << FileSystem::getErrorString(ret) << endl
             << endl
             << "operation fail! " << endl;
        return ret;
    }

    AbstractFileSystem *dstFs;
    ret = FileSystem::parseInternal(dstPath, dstFs);
    if (ret != EC_OK) {
        cerr << "copy src <" << srcPath << "> to dst <" << dstPath << "> fail. parse dst file fail."
             << FileSystem::getErrorString(ret) << endl
             << endl
             << "operation fail! " << endl;
        return ret;
    }

    PathInfo srcInfo(srcFs, srcPath);
    PathInfo dstInfo(dstFs, dstPath);
    if (FileSystem::isZkLikeFileSystem(srcFs) && !FileSystem::isZkLikeFileSystem(dstFs)) {
        ret = FileSystem::copyZKLikeFsToOtherFs(srcInfo, dstInfo, recursive, true);
        if (ret == EC_OK) {
            cout << endl << "operation success!" << endl;
            return ret;
        }
    } else {
        ret = FileSystem::copyAll(srcInfo, dstInfo, recursive, true);
        if (ret == EC_OK) {
            cout << endl << "operation success!" << endl;
            return ret;
        }
    }

    cerr << endl << "operation fail! " << endl;
    return ret;
}

ErrorCode FsUtil::runEncrypt(const char *srcPath, const char *dstPath, const char *cipherOption, bool optionUseBase64) {
    autil::cipher::CipherOption option(autil::cipher::CipherType::CT_UNKNOWN);
    if (cipherOption == nullptr) {
        if (!getCipherOption(option)) {
            return EC_BADARGS;
        }
    } else {
        if (!option.fromKVString(cipherOption, optionUseBase64)) {
            return EC_BADARGS;
        }
    }

    auto beginTs = autil::TimeUtility::currentTime();
    cout << "-------------------------------------------------" << endl;
    FileMeta fileMeta;
    ErrorCode ret = FileSystem::getFileMeta(srcPath, fileMeta);
    if (ret != EC_OK) {
        cerr << "fail to get length of file <" << srcPath << ">. " << FileSystem::getErrorString(ret) << endl;
        return ret;
    }
    unique_ptr<File> rFile(FileSystem::openFile(srcPath, READ));
    if (!rFile->isOpened()) {
        cerr << "open file <" << srcPath << "> for read fail. " << FileSystem::getErrorString(rFile->getLastError())
             << endl;
        return rFile->getLastError();
    }

    if (std::string(dstPath) != "/dev/null" && FileSystem::isExist(dstPath) == EC_TRUE) {
        cerr << "target path <" << dstPath << "> already exist. " << endl;
        return EC_BADARGS;
    }
    unique_ptr<File> wFile(FileSystem::openFile(dstPath, WRITE));
    if (!wFile->isOpened()) {
        cerr << "open file <" << dstPath << "> for write fail. " << FileSystem::getErrorString(wFile->getLastError())
             << endl;
        return wFile->getLastError();
    }

    volatile bool hasError = false;
    ErrorCode rCode = EC_OK;
    auto encrypter = autil::cipher::AESCipherCreator::createStreamEncrypter(option, 2 * 1024 * 1024 /* 2 MB */);
    if (encrypter == nullptr) {
        cerr << "create stream encrypter fail." << endl;
        return EC_UNKNOWN;
    }

    Md5StreamPtr rFileMd5;
    Md5StreamPtr wFileMd5;
    if (autil::EnvUtil::getEnv("PRINT_MD5", false)) {
        rFileMd5.reset(new Md5Stream);
        wFileMd5.reset(new Md5Stream);
    }
    size_t totalEncryptLen = 0;
    std::thread readThread([&]() {
        size_t bufferLen = 2 * 1024 * 1024;
        SafeBuffer buffer(bufferLen);
        while (!hasError) {
            auto readLen = rFile->read(buffer.getBuffer(), bufferLen);
            if (readLen < 0) {
                cerr << "read file <" << srcPath << ">  fail. " << FileSystem::getErrorString(rFile->getLastError())
                     << endl;
                rCode = rFile->getLastError();
                hasError = true;
                continue;
            }
            if (readLen > 0) {
                if (rFileMd5) {
                    rFileMd5->Put((const uint8_t *)buffer.getBuffer(), readLen);
                }
                if (!encrypter->append((const unsigned char *)buffer.getBuffer(), readLen)) {
                    cerr << "encrypter append data fail." << endl;
                    rCode = EC_UNKNOWN;
                    hasError = true;
                    continue;
                }
            }
            totalEncryptLen += readLen;
            if (rFile->isEof()) {
                break;
            }
        }
        if (!encrypter->seal()) {
            cerr << "encrypter seal fail." << endl;
            rCode = EC_UNKNOWN;
            hasError = true;
        }
        rFile->close();
    });

    ErrorCode wCode = EC_OK;
    size_t totalWriteLen = 0;
    std::thread writeThread([&]() {
        size_t bufferLen = 2 * 1024 * 1024;
        SafeBuffer buffer(bufferLen);
        while (!hasError) {
            auto ret = encrypter->get((unsigned char *)buffer.getBuffer(), bufferLen);
            if (ret > 0) {
                if (wFileMd5) {
                    wFileMd5->Put((const uint8_t *)buffer.getBuffer(), ret);
                }
                if (wFile->write(buffer.getBuffer(), ret) != ret) {
                    cerr << "write to dst file <" << dstPath << "> fail.  "
                         << FileSystem::getErrorString(wFile->getLastError()) << endl;
                    wCode = wFile->getLastError();
                    hasError = true;
                }
                totalWriteLen += ret;
                continue;
            }
            if (encrypter->isEof()) {
                break;
            }
        }
        wCode = wFile->close();
        if (wCode != EC_OK) {
            cerr << "close dst file <" << dstPath << "> fail.  " << FileSystem::getErrorString(wFile->getLastError())
                 << endl;
            hasError = true;
        }
    });

    readThread.join();
    writeThread.join();
    if (hasError) {
        return rCode != EC_OK ? rCode : wCode;
    }
    auto interval = autil::TimeUtility::currentTime() - beginTs;
    cout << "encryption interval:" << (double)interval / 1000000 << " seconds" << endl;
    cout << "plain file length:" << totalEncryptLen << ", cipher file length:" << totalWriteLen << endl;
    cout << "=================================================" << endl;
    cout << "== key hex:" << encrypter->getKeyHexString() << endl;
    cout << "== iv hex:" << encrypter->getIvHexString() << endl;
    cout << "== salt hex:" << encrypter->getSaltHexString() << endl;

    if (rFileMd5 && wFileMd5) {
        cout << "++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
        cout << "++ src file md5:" << rFileMd5->GetMd5String() << endl;
        cout << "++ dst file md5:" << wFileMd5->GetMd5String() << endl;
    }
    return EC_OK;
}

ErrorCode FsUtil::runDecrypt(const char *srcPath, const char *dstPath, const char *cipherOption, bool optionUseBase64) {
    autil::cipher::CipherOption option(autil::cipher::CipherType::CT_UNKNOWN);
    if (cipherOption == nullptr) {
        if (!getCipherOption(option)) {
            return EC_BADARGS;
        }
    } else {
        if (!option.fromKVString(cipherOption, optionUseBase64)) {
            return EC_BADARGS;
        }
    }

    auto beginTs = autil::TimeUtility::currentTime();
    cout << "-------------------------------------------------" << endl;
    FileMeta fileMeta;
    ErrorCode ret = FileSystem::getFileMeta(srcPath, fileMeta);
    if (ret != EC_OK) {
        cerr << "fail to get length of file <" << srcPath << ">. " << FileSystem::getErrorString(ret) << endl;
        return ret;
    }
    unique_ptr<File> rFile(FileSystem::openFile(srcPath, READ));
    if (!rFile->isOpened()) {
        cerr << "open file <" << srcPath << "> for read fail. " << FileSystem::getErrorString(rFile->getLastError())
             << endl;
        return rFile->getLastError();
    }

    if (std::string(dstPath) != "/dev/null" && FileSystem::isExist(dstPath) == EC_TRUE) {
        cerr << "target path <" << dstPath << "> already exist. " << endl;
        return EC_BADARGS;
    }
    unique_ptr<File> wFile(FileSystem::openFile(dstPath, WRITE));
    if (!wFile->isOpened()) {
        cerr << "open file <" << dstPath << "> for write fail. " << FileSystem::getErrorString(wFile->getLastError())
             << endl;
        return wFile->getLastError();
    }

    volatile bool hasError = false;
    ErrorCode rCode = EC_OK;
    auto decrypter = autil::cipher::AESCipherCreator::createStreamDecrypter(option, 2 * 1024 * 1024 /* 2 MB */);
    if (decrypter == nullptr) {
        cerr << "create stream decrypter fail." << endl;
        return EC_UNKNOWN;
    }

    Md5StreamPtr rFileMd5;
    Md5StreamPtr wFileMd5;
    if (autil::EnvUtil::getEnv("PRINT_MD5", false)) {
        rFileMd5.reset(new Md5Stream);
        wFileMd5.reset(new Md5Stream);
    }

    size_t totalDecryptLen = 0;
    std::thread readThread([&]() {
        size_t bufferLen = 2 * 1024 * 1024;
        SafeBuffer buffer(bufferLen);
        while (!hasError) {
            auto readLen = rFile->read(buffer.getBuffer(), bufferLen);
            if (readLen < 0) {
                cerr << "read file <" << srcPath << ">  fail. " << FileSystem::getErrorString(rFile->getLastError())
                     << endl;
                rCode = rFile->getLastError();
                hasError = true;
                continue;
            }
            if (readLen > 0) {
                if (rFileMd5) {
                    rFileMd5->Put((const uint8_t *)buffer.getBuffer(), readLen);
                }
                if (!decrypter->append((const unsigned char *)buffer.getBuffer(), readLen)) {
                    cerr << "decrypter append data fail." << endl;
                    rCode = EC_UNKNOWN;
                    hasError = true;
                    continue;
                }
            }
            totalDecryptLen += readLen;
            if (rFile->isEof()) {
                break;
            }
        }
        if (!decrypter->seal()) {
            cerr << "decrypter seal fail." << endl;
            rCode = EC_UNKNOWN;
            hasError = true;
        }
        rFile->close();
    });

    ErrorCode wCode = EC_OK;
    size_t totalWriteLen = 0;
    std::thread writeThread([&]() {
        size_t bufferLen = 2 * 1024 * 1024;
        SafeBuffer buffer(bufferLen);
        while (!hasError) {
            auto ret = decrypter->get((unsigned char *)buffer.getBuffer(), bufferLen);
            if (ret > 0) {
                if (wFileMd5) {
                    wFileMd5->Put((const uint8_t *)buffer.getBuffer(), ret);
                }
                if (wFile->write(buffer.getBuffer(), ret) != ret) {
                    cerr << "write to dst file <" << dstPath << "> fail.  "
                         << FileSystem::getErrorString(wFile->getLastError()) << endl;
                    wCode = wFile->getLastError();
                    hasError = true;
                }
                totalWriteLen += ret;
                continue;
            }
            if (decrypter->isEof()) {
                break;
            }
        }
        wCode = wFile->close();
        if (wCode != EC_OK) {
            cerr << "close dst file <" << dstPath << "> fail.  " << FileSystem::getErrorString(wFile->getLastError())
                 << endl;
            hasError = true;
        }
    });

    readThread.join();
    writeThread.join();
    if (hasError) {
        return rCode != EC_OK ? rCode : wCode;
    }

    auto interval = autil::TimeUtility::currentTime() - beginTs;
    cout << "decryption interval:" << (double)interval / 1000000 << " seconds" << endl;
    cout << "cipher file length:" << totalDecryptLen << ", plain file length:" << totalWriteLen << endl;
    cout << "=================================================" << endl;
    cout << "== key hex:" << decrypter->getKeyHexString() << endl;
    cout << "== iv hex:" << decrypter->getIvHexString() << endl;
    cout << "== salt hex:" << decrypter->getSaltHexString() << endl;
    if (rFileMd5 && wFileMd5) {
        cout << "++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
        cout << "++ src file md5:" << rFileMd5->GetMd5String() << endl;
        cout << "++ dst file md5:" << wFileMd5->GetMd5String() << endl;
    }
    return EC_OK;
}

class AESCipherDataReader {
public:
    AESCipherDataReader(std::unique_ptr<autil::cipher::AESCipherStreamDecrypter> &decrypter, volatile bool &hasError)
        : _hasError(hasError), _decrypter(decrypter) {}
    ssize_t read(char *output, uint32_t size) {
        if (_decrypter == nullptr) {
            return -1;
        }
        size_t sizeUsed = 0;
        while (sizeUsed < size) {
            uint32_t sizeToRead = size - sizeUsed;
            char *buffer = output + sizeUsed;
            uint32_t sizeRead = 0;
            if (!innerGet(buffer, sizeToRead, sizeRead)) {
                return _hasError ? -1 : sizeUsed;
            }
            sizeUsed += sizeRead;
        }
        return sizeUsed;
    }
    bool isEof() const { return _decrypter->isEof(); }

private:
    bool innerGet(char *output, uint32_t size, uint32_t &sizeUsed) {
        assert(_decrypter != nullptr);
        while (!_hasError) {
            auto ret = _decrypter->get((unsigned char *)output, size);
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

private:
    volatile bool &_hasError;
    std::unique_ptr<autil::cipher::AESCipherStreamDecrypter> &_decrypter;
};

ErrorCode FsUtil::runDecryptZCat(const char *srcPath, const char *cipherOption, bool optionUseBase64) {
    autil::cipher::CipherOption option(autil::cipher::CipherType::CT_UNKNOWN);
    if (cipherOption == nullptr) {
        if (!getCipherOption(option)) {
            return EC_BADARGS;
        }
    } else {
        if (!option.fromKVString(cipherOption, optionUseBase64)) {
            return EC_BADARGS;
        }
    }

    FileMeta fileMeta;
    ErrorCode ret = FileSystem::getFileMeta(srcPath, fileMeta);
    if (ret != EC_OK) {
        cerr << "fail to get length of file <" << srcPath << ">. " << FileSystem::getErrorString(ret) << endl;
        return ret;
    }
    unique_ptr<File> rFile(FileSystem::openFile(srcPath, READ));
    if (!rFile->isOpened()) {
        cerr << "open file <" << srcPath << "> for read fail. " << FileSystem::getErrorString(rFile->getLastError())
             << endl;
        return rFile->getLastError();
    }
    volatile bool hasError = false;
    ErrorCode rCode = EC_OK;
    auto decrypter = autil::cipher::AESCipherCreator::createStreamDecrypter(option, 2 * 1024 * 1024 /* 2 MB */);
    if (decrypter == nullptr) {
        cerr << "create stream decrypter fail." << endl;
        return EC_UNKNOWN;
    }
    size_t totalDecryptLen = 0;
    std::thread readThread([&]() {
        size_t bufferLen = 2 * 1024 * 1024;
        SafeBuffer buffer(bufferLen);
        while (!hasError) {
            auto readLen = rFile->read(buffer.getBuffer(), bufferLen);
            if (readLen < 0) {
                cerr << "read file <" << srcPath << ">  fail. " << FileSystem::getErrorString(rFile->getLastError())
                     << endl;
                rCode = rFile->getLastError();
                hasError = true;
                continue;
            }
            if (readLen > 0) {
                if (!decrypter->append((const unsigned char *)buffer.getBuffer(), readLen)) {
                    cerr << "decrypter append data fail." << endl;
                    rCode = EC_UNKNOWN;
                    hasError = true;
                    continue;
                }
            }
            totalDecryptLen += readLen;
            if (rFile->isEof()) {
                break;
            }
        }
        if (!decrypter->seal()) {
            cerr << "decrypter seal fail." << endl;
            rCode = EC_UNKNOWN;
            hasError = true;
        }
    });

    ErrorCode wCode = EC_OK;
    std::thread writeThread([&]() {
        std::unique_ptr<AESCipherDataReader> reader(new AESCipherDataReader(decrypter, hasError));
        if (!GZipHeaderParser<AESCipherDataReader>::parse(reader)) {
            hasError = true;
            wCode = EC_UNKNOWN;
            return;
        }
        z_stream strm;
        strm.zalloc = Z_NULL;
        strm.zfree = Z_NULL;
        strm.opaque = Z_NULL;
        strm.avail_in = 0;
        strm.next_in = Z_NULL;
        if (inflateInit2(&strm, -MAX_WBITS) != Z_OK) {
            cerr << "gzip infalte init error" << endl;
            hasError = true;
            wCode = EC_UNKNOWN;
            return;
        }
        size_t bufferLen = 2 * 1024 * 1024;
        SafeBuffer buffer(bufferLen + 1);
        SafeBuffer outBuffer(bufferLen);
        while (true) {
            auto readLen = reader->read(buffer.getBuffer(), bufferLen);
            if (readLen < 0) {
                cerr << "read decrypted file data fail. " << endl;
                inflateEnd(&strm);
                hasError = true;
                wCode = EC_UNKNOWN;
                return;
            }
            if (readLen > 0) {
                char *inBuf = buffer.getBuffer();
                char *outBuf = outBuffer.getBuffer();
                inBuf[readLen] = 0;
                uint32_t bufferNow = 0;
                uint32_t bufferEnd = readLen;
                while (bufferNow < bufferEnd) {
                    strm.avail_out = bufferLen;
                    strm.next_out = (Bytef *)outBuf;
                    strm.avail_in = bufferEnd - bufferNow;
                    strm.next_in = (Bytef *)&inBuf[bufferNow];
                    int ret = inflate(&strm, Z_NO_FLUSH);
                    if ((ret != Z_STREAM_END) && (ret != Z_OK)) {
                        cerr << "infalte error(" << ret << ")" << endl;
                        inflateEnd(&strm);
                        hasError = true;
                        wCode = EC_UNKNOWN;
                        return;
                    }
                    bufferNow = bufferEnd - strm.avail_in;
                    auto dSize = bufferLen - strm.avail_out;
                    fwrite(outBuf, sizeof(char), dSize, stdout);
                    if (ret == Z_STREAM_END) {
                        inflateEnd(&strm);
                        return;
                    }
                }
            }
            if (reader->isEof()) {
                break;
            }
        }
        inflateEnd(&strm);
    });
    readThread.join();
    writeThread.join();
    if (hasError) {
        return rCode != EC_OK ? rCode : wCode;
    }
    return EC_OK;
}

ErrorCode FsUtil::runDecryptCat(const char *srcPath, const char *cipherOption, bool optionUseBase64) {
    autil::cipher::CipherOption option(autil::cipher::CipherType::CT_UNKNOWN);
    if (cipherOption == nullptr) {
        if (!getCipherOption(option)) {
            return EC_BADARGS;
        }
    } else {
        if (!option.fromKVString(cipherOption, optionUseBase64)) {
            return EC_BADARGS;
        }
    }

    FileMeta fileMeta;
    ErrorCode ret = FileSystem::getFileMeta(srcPath, fileMeta);
    if (ret != EC_OK) {
        cerr << "fail to get length of file <" << srcPath << ">. " << FileSystem::getErrorString(ret) << endl;
        return ret;
    }
    unique_ptr<File> rFile(FileSystem::openFile(srcPath, READ));
    if (!rFile->isOpened()) {
        cerr << "open file <" << srcPath << "> for read fail. " << FileSystem::getErrorString(rFile->getLastError())
             << endl;
        return rFile->getLastError();
    }

    volatile bool hasError = false;
    ErrorCode rCode = EC_OK;
    auto decrypter = autil::cipher::AESCipherCreator::createStreamDecrypter(option, 2 * 1024 * 1024 /* 2 MB */);
    if (decrypter == nullptr) {
        cerr << "create stream decrypter fail." << endl;
        return EC_UNKNOWN;
    }
    size_t totalDecryptLen = 0;
    std::thread readThread([&]() {
        size_t bufferLen = 2 * 1024 * 1024;
        SafeBuffer buffer(bufferLen);
        while (!hasError) {
            auto readLen = rFile->read(buffer.getBuffer(), bufferLen);
            if (readLen < 0) {
                cerr << "read file <" << srcPath << ">  fail. " << FileSystem::getErrorString(rFile->getLastError())
                     << endl;
                rCode = rFile->getLastError();
                hasError = true;
                continue;
            }
            if (readLen > 0) {
                if (!decrypter->append((const unsigned char *)buffer.getBuffer(), readLen)) {
                    cerr << "decrypter append data fail." << endl;
                    rCode = EC_UNKNOWN;
                    hasError = true;
                    continue;
                }
            }
            totalDecryptLen += readLen;
            if (rFile->isEof()) {
                break;
            }
        }
        if (!decrypter->seal()) {
            cerr << "decrypter seal fail." << endl;
            rCode = EC_UNKNOWN;
            hasError = true;
        }
    });

    std::thread writeThread([&]() {
        size_t bufferLen = 2 * 1024 * 1024;
        SafeBuffer buffer(bufferLen);
        while (!hasError) {
            auto ret = decrypter->get((unsigned char *)buffer.getBuffer(), bufferLen);
            if (ret > 0) {
                fwrite(buffer.getBuffer(), sizeof(char), ret, stdout);
                continue;
            }
            if (decrypter->isEof()) {
                break;
            }
        }
    });
    readThread.join();
    writeThread.join();
    return rCode;
}

template <typename T>
bool FsUtil::getUserConsoleInput(const std::string &hintStr,
                                 const std::set<std::string> &validInputs,
                                 const std::string &defaultValue,
                                 size_t maxInputTimes,
                                 T &value) {
    bool ret = false;
    std::string inputStr;
    size_t cnt = 0;
    while (true) {
        if (cnt >= maxInputTimes) {
            std::cout << "reach max input times: " << maxInputTimes << "!" << endl;
            break;
        }
        std::cout << hintStr;
        std::getline(cin, inputStr);
        ++cnt;
        if (inputStr.empty() && !defaultValue.empty()) {
            inputStr = defaultValue;
            std::cout << "use default input value [" << defaultValue << "]" << endl;
        }
        if (inputStr.empty()) {
            std::cout << "invalid empty input: [" << inputStr << "]" << endl;
            continue;
        }
        if (!validInputs.empty() && validInputs.find(inputStr) == validInputs.end()) {
            std::cout << "invalid input: [" << inputStr
                      << "], candidates : " << autil::StringUtil::toString(validInputs, "|") << endl;
            continue;
        }
        if (!autil::StringUtil::fromString<T>(inputStr, value)) {
            std::cout << "invalid input: [" << inputStr << "], wrong type!" << endl;
            continue;
        }
        ret = true;
        break;
    }
    return ret;
}

bool FsUtil::getCipherOption(autil::cipher::CipherOption &option) {
    int maxInputTimes = 3;
    std::string typeStr;
    if (!getUserConsoleInput("\n1. input cipher type, enter for default [aes-128-cbc] :",
                             {"aes-128-cbc", "aes-192-cbc", "aes-256-cbc", "aes-128-ctr", "aes-192-ctr", "aes-256-ctr"},
                             "aes-128-cbc",
                             maxInputTimes,
                             typeStr)) {
        return false;
    }

    autil::cipher::CipherType type = autil::cipher::CipherOption::parseCipherType(typeStr);
    if (type == autil::cipher::CipherType::CT_UNKNOWN) {
        std::cout << "invalid cipher type string [" << typeStr << "]" << std::endl;
        return false;
    }

    std::string mode;
    if (!getUserConsoleInput("\n2. select cipher mode [passwd|key], enter for default[passwd] :",
                             {"passwd", "key"},
                             "passwd",
                             maxInputTimes,
                             mode)) {
        return false;
    }

    if (mode == "passwd") {
        std::string passwd;
        getUserConsoleInput("\n3. input password:", {}, "", maxInputTimes, passwd);
        std::string digist;
        if (!getUserConsoleInput("\n4. select digist [md5|sha256], enter for default[md5] :",
                                 {"md5", "sha256"},
                                 "md5",
                                 maxInputTimes,
                                 digist)) {
            return false;
        }
        autil::cipher::DigistType dgstType = autil::cipher::CipherOption::parseDigistType(digist);
        if (dgstType == autil::cipher::DigistType::DT_UNKNOWN) {
            std::cout << "invalid digist [" << digist << "]" << endl;
            return false;
        }
        std::string usePbkdf2;
        if (!getUserConsoleInput("\n5. use pbkdf2 ? yes or no, enter for default [no] :",
                                 {"yes", "no"},
                                 "no",
                                 maxInputTimes,
                                 usePbkdf2)) {
            return false;
        }

        std::string needSaltStr;
        if (!getUserConsoleInput("\n6. need salt ? yes or no, enter for default [yes] :",
                                 {"yes", "no"},
                                 "yes",
                                 maxInputTimes,
                                 needSaltStr)) {
            return false;
        }
        if (needSaltStr == "no") {
            option = autil::cipher::CipherOption::password(type, dgstType, passwd, usePbkdf2 == "yes");
            return true;
        }

        std::string useRandomSaltStr;
        if (!getUserConsoleInput("\n7. use random salt ? yes or no, enter for default [yes] :",
                                 {"yes", "no"},
                                 "yes",
                                 maxInputTimes,
                                 useRandomSaltStr)) {
            return false;
        }
        if (useRandomSaltStr == "yes") {
            option = autil::cipher::CipherOption::passwordWithRandomSalt(type, dgstType, passwd, usePbkdf2 == "yes");
            return true;
        }
        std::string useDefineSalt;
        getUserConsoleInput("\n8. input salt hex string:", {}, "", maxInputTimes, useDefineSalt);
        option =
            autil::cipher::CipherOption::passwordWithSalt(type, dgstType, passwd, useDefineSalt, usePbkdf2 == "yes");
        return true;
    }

    if (mode == "key") {
        std::string keyStr;
        getUserConsoleInput("\n3. input key hex string:", {}, "", maxInputTimes, keyStr);
        std::string ivStr;
        getUserConsoleInput("\n4. input iv hex string:", {}, "", maxInputTimes, ivStr);
        option = autil::cipher::CipherOption::secretKey(type, keyStr, ivStr);
        return true;
    }
    assert(false);
    return false;
}

ErrorCode FsUtil::runMove(const char *srcPath, const char *dstPath) {
    ErrorCode ret = FileSystem::moveInternal(srcPath, dstPath, true);
    if (ret == EC_OK) {
        cout << endl << "operation success!" << endl;
    } else {
        cerr << endl << "operation fail! " << endl;
    }
    return ret;
}

ErrorCode FsUtil::runRename(const char *srcPath, const char *dstPath) {
    ErrorCode ret = FileSystem::rename(srcPath, dstPath);
    if (ret == EC_OK) {
        cout << endl << "operation success!" << endl;
    } else {
        cerr << endl << "operation fail! " << endl;
    }
    return ret;
}

ErrorCode FsUtil::runMkDir(const char *dirname, bool recursive) {
    ErrorCode ret = FileSystem::mkDir(dirname, recursive);
    if (ret != EC_OK) {
        cerr << "mkdir <" << dirname << "> fail! " << FileSystem::getErrorString(ret) << endl;
    } else {
        cout << "operation success!" << endl;
    }
    return ret;
}

ErrorCode FsUtil::runListDir(const char *dirname) {
    FileList fileList;
    ErrorCode ret = FileSystem::listDir(dirname, fileList);
    if (ret != EC_OK) {
        cerr << "list dir <" << dirname << "> fail! " << FileSystem::getErrorString(ret) << endl;
    } else {
        sort(fileList.begin(), fileList.end());
        for (size_t i = 0; i < fileList.size(); i++) {
            cout << fileList[i] << endl;
        }
    }
    return ret;
}

ErrorCode FsUtil::runListDirWithRecursive(const char *dirname, const char *threadNumStr) {
    int32_t threadNum;
    if (strcmp(threadNumStr, "") == 0) {
        threadNum = DEFAULT_THREAD_NUM;
    } else {
        if (!autil::StringUtil::strToInt32(threadNumStr, threadNum)) {
            cerr << "parse threadNum[" << threadNumStr << "] Error!" << endl;
            return EC_UNKNOWN;
        }
    }
    EntryInfoMap entryMap;
    ErrorCode ret = FileSystem::listDir(dirname, entryMap, threadNum);
    if (ret != EC_OK) {
        cerr << "list dir <" << dirname << "> fail! " << endl;
    } else {
        EntryInfoMap::const_iterator it = entryMap.begin();
        for (; it != entryMap.end(); it++) {
            cout << it->first << endl;
        }
    }
    return ret;
}

ErrorCode FsUtil::runRemove(const char *path) {
    ErrorCode ret = FileSystem::remove(path);
    if (ret != EC_OK) {
        cerr << "remove path <" << path << "> fail! " << FileSystem::getErrorString(ret) << endl;
    } else {
        cout << "operation success!" << endl;
    }
    return ret;
}

ErrorCode FsUtil::runIsExist(const char *pathname) {
    ErrorCode ret = FileSystem::isExist(pathname);
    if (ret == EC_TRUE) {
        cout << "path exist!" << endl;
    } else if (ret == EC_FALSE) {
        cout << "path does not exist!" << endl;
    } else {
        cerr << FileSystem::getErrorString(ret) << endl << "operation fail!" << endl;
    }

    return ret;
}

ErrorCode FsUtil::runMd5Sum(const char *fileName) {
    FileMeta fileMeta;
    std::string fileNameCopy = std::string(fileName);
    ErrorCode ret = FileSystem::getFileMeta(fileNameCopy, fileMeta);
    if (ret != EC_OK) {
        cerr << "fail to get length of file <" << fileNameCopy << ">. " << FileSystem::getErrorString(ret) << endl;
        return ret;
    }
    unique_ptr<File> file(FileSystem::openFile(fileNameCopy, READ));
    if (!file->isOpened()) {
        cerr << "open file <" << fileNameCopy << "> for read fail. " << FileSystem::getErrorString(file->getLastError())
             << endl;
        return file->getLastError();
    }
    int64_t len = fileMeta.fileLength;
    if (len == 0) {
        return EC_OK;
    }

    Md5StreamPtr rFileMd5(new Md5Stream);
    size_t bufferLen = 2 * 1024 * 1024;
    SafeBuffer buffer(bufferLen);
    while (true) {
        auto readLen = file->read(buffer.getBuffer(), bufferLen);
        if (readLen < 0) {
            cerr << "read file <" << fileNameCopy << ">  fail. " << FileSystem::getErrorString(file->getLastError())
                 << endl;
            return file->getLastError();
        }
        if (readLen > 0) {
            rFileMd5->Put((const uint8_t *)buffer.getBuffer(), readLen);
        }
        if (file->isEof()) {
            break;
        }
    }
    file->close();
    cout << "file md5:" << rFileMd5->GetMd5String() << endl;
    return EC_OK;
}

ErrorCode FsUtil::runCat(const char *fileName) {
    FileMeta fileMeta;
    std::string fileNameCopy = std::string(fileName);
    ErrorCode ret = FileSystem::getFileMeta(fileNameCopy, fileMeta);
    if (ret != EC_OK) {
        cerr << "fail to get length of file <" << fileNameCopy << ">. " << FileSystem::getErrorString(ret) << endl;
        return ret;
    }
    unique_ptr<File> file(FileSystem::openFile(fileNameCopy, READ));
    if (!file->isOpened()) {
        cerr << "open file <" << fileNameCopy << "> for read fail. " << FileSystem::getErrorString(file->getLastError())
             << endl;
        return file->getLastError();
    }
    int64_t len = fileMeta.fileLength;
    if (len == 0) {
        return EC_OK;
    }

    size_t bufferLen = 2 * 1024 * 1024;
    SafeBuffer buffer(bufferLen);
    while (true) {
        auto readLen = file->read(buffer.getBuffer(), bufferLen);
        if (readLen < 0) {
            cerr << "read file <" << fileNameCopy << ">  fail. " << FileSystem::getErrorString(file->getLastError())
                 << endl;
            return file->getLastError();
        }
        if (readLen > 0) {
            fwrite(buffer.getBuffer(), sizeof(char), readLen, stdout);
        }
        if (file->isEof()) {
            break;
        }
    }
    file->close();
    return EC_OK;
}

ErrorCode FsUtil::runZCat(const char *fileName) {
    FileMeta fileMeta;
    std::string fileNameCopy = std::string(fileName);
    ErrorCode ret = FileSystem::getFileMeta(fileNameCopy, fileMeta);
    if (ret != EC_OK) {
        cerr << "fail to get length of file <" << fileNameCopy << ">. " << FileSystem::getErrorString(ret) << endl;
        return ret;
    }
    unique_ptr<File> file(FileSystem::openFile(fileNameCopy, READ));
    if (!file->isOpened()) {
        cerr << "open file <" << fileNameCopy << "> for read fail. " << FileSystem::getErrorString(file->getLastError())
             << endl;
        return file->getLastError();
    }
    if (fileMeta.fileLength == 0) {
        return EC_OK;
    }
    if (!GZipHeaderParser<File>::parse(file)) {
        return EC_UNKNOWN;
    }
    z_stream strm;
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    if (inflateInit2(&strm, -MAX_WBITS) != Z_OK) {
        cerr << "gzip infalte init error" << endl;
        return EC_UNKNOWN;
    }
    size_t bufferLen = 2 * 1024 * 1024;
    SafeBuffer buffer(bufferLen + 1);
    SafeBuffer outBuffer(bufferLen);
    while (true) {
        auto readLen = file->read(buffer.getBuffer(), bufferLen);
        if (readLen < 0) {
            cerr << "read file <" << fileNameCopy << ">  fail. " << FileSystem::getErrorString(file->getLastError())
                 << endl;
            inflateEnd(&strm);
            return file->getLastError();
        }
        if (readLen > 0) {
            char *inBuf = buffer.getBuffer();
            char *outBuf = outBuffer.getBuffer();
            inBuf[readLen] = 0;
            uint32_t bufferNow = 0;
            uint32_t bufferEnd = readLen;
            while (bufferNow < bufferEnd) {
                strm.avail_out = bufferLen;
                strm.next_out = (Bytef *)outBuf;
                strm.avail_in = bufferEnd - bufferNow;
                strm.next_in = (Bytef *)&inBuf[bufferNow];
                int ret = inflate(&strm, Z_NO_FLUSH);
                if ((ret != Z_STREAM_END) && (ret != Z_OK)) {
                    cerr << "infalte error(" << ret << ")" << endl;
                    inflateEnd(&strm);
                    return EC_UNKNOWN;
                }
                bufferNow = bufferEnd - strm.avail_in;
                auto dSize = bufferLen - strm.avail_out;
                fwrite(outBuf, sizeof(char), dSize, stdout);
                if (ret == Z_STREAM_END) {
                    inflateEnd(&strm);
                    file->close();
                    return EC_OK;
                }
            }
        }
        if (file->isEof()) {
            break;
        }
    }
    inflateEnd(&strm);
    file->close();
    return EC_OK;
}

ErrorCode FsUtil::getPathMeta(const string &pathName, FileMeta &meta) {
    ErrorCode ret = FileSystem::getFileMeta(pathName, meta);
    if (ret != EC_OK) {
        cerr << "get metainfo of file <" << pathName << "> fail!" << FileSystem::getErrorString(ret) << endl;
    }
    return ret;
}

ErrorCode FsUtil::getDirMeta(const string &pathName, uint32_t &fileCount, uint32_t &dirCount, uint64_t &size) {
    FileList fileList;
    ErrorCode ret = FileSystem::listDir(pathName, fileList);
    if (ret != EC_OK) {
        cerr << "list dir <" << pathName << "> fail!" << FileSystem::getErrorString(ret) << endl;
        return ret;
    }

    string subPath;
    for (size_t i = 0; i < fileList.size(); i++) {
        if (!FileSystem::appendPath(pathName, fileList[i], subPath)) {
            cerr << "fail to get subpath. "
                 << "parent path <" << pathName << ">, patname <" << fileList[i] << ">" << endl;
            return EC_PARSEFAIL;
        }

        ret = FileSystem::isFile(subPath);
        if (ret == EC_TRUE) {
            fileCount++;

            FileMeta meta;
            ret = getPathMeta(subPath, meta);
            if (ret != EC_OK) {
                return ret;
            }

            size += meta.fileLength;
        } else if (ret == EC_FALSE) {
            ret = FileSystem::isDirectory(subPath);
            if (ret == EC_TRUE) {
                dirCount++;
                uint32_t subFileCount = 0;
                uint32_t subDirCount = 0;
                ret = getDirMeta(subPath, subFileCount, subDirCount, size);
                if (ret != EC_OK) {
                    return ret;
                }
            } else if (ret == EC_FALSE) {
                cerr << "path <" << subPath << "> is neither a file nor directory" << endl;
                return EC_NOTSUP;
            } else {
                cerr << "get path <" << subPath << "> meta fail, " << FileSystem::getErrorString(ret) << endl;
                return ret;
            }
        } else {
            cerr << "get path <" << subPath << "> meta fail, " << FileSystem::getErrorString(ret) << endl;
            return ret;
        }
    }

    return EC_OK;
}

#define CONVERT_ZOO_TIME(meta, timeType, atime)                                                                        \
    while (true) {                                                                                                     \
        time_t time = meta.timeType / 1000;                                                                            \
        struct tm tmpTm;                                                                                               \
        localtime_r(&time, &tmpTm);                                                                                    \
        strftime(atime, 30, "%Y-%m-%d %T", &tmpTm);                                                                    \
        break;                                                                                                         \
    }

ErrorCode FsUtil::runGetMeta(const char *pathName) {
    ErrorCode ret = EC_OK;
    if (FileSystem::getFsType(pathName) == "zfs") {
        cout << "this path is a zookeeper node." << endl;

        uint32_t totalNodeCount = 0;
        uint64_t size = 0;
        FileMeta meta;
        ret = getPathMeta(pathName, meta);
        if (ret != EC_OK) {
            cerr << endl << "operation fail!" << endl;
            return ret;
        }

        ret = getZookeeperNodeMeta(pathName, totalNodeCount, size);
        if (ret == EC_OK) {
            cout << "current node size: " << meta.fileLength << endl;
            cout << "total sub node count: " << totalNodeCount << endl;
            cout << "total sub node size: " << size << endl;
            char createTime[30];
            char modifyTime[30];
            memset(createTime, '\0', sizeof(createTime));
            memset(createTime, '\0', sizeof(modifyTime));
            CONVERT_ZOO_TIME(meta, createTime, createTime);
            CONVERT_ZOO_TIME(meta, lastModifyTime, modifyTime);
            cout << "node create time: " << string(createTime) << endl;
            cout << "node last modify time: " << string(modifyTime) << endl;
        } else {
            cerr << endl << "operation fail!" << endl;
        }
        return ret;
    }

    ret = FileSystem::isDirectory(pathName);
    if (ret == EC_TRUE) {
        cout << "this path is a directory." << endl;

        uint32_t fileCount = 0;
        uint32_t dirCount = 0;
        uint64_t size = 0;
        ret = getDirMeta(pathName, fileCount, dirCount, size);
        if (ret == EC_OK) {
            cout << "file count: " << fileCount << endl;
            cout << "dir count: " << dirCount << endl;
            cout << "total size: " << size << endl;
            return ret;
        } else {
            cerr << endl << "operation fail!" << endl;
            return ret;
        }
    } else if (ret == EC_FALSE) {
        ret = FileSystem::isFile(pathName);
        if (ret == EC_TRUE) {
            cout << "this path is a file." << endl;

            FileMeta fileMeta;
            ret = getPathMeta(pathName, fileMeta);
            if (ret == EC_OK) {
                cout << "file length: " << fileMeta.fileLength << endl;
                char buffer[30];
                timeToStr(fileMeta.createTime, buffer);
                cout << "file create time: " << buffer << endl;
                timeToStr(fileMeta.lastModifyTime, buffer);
                cout << "file last modify time: " << buffer << endl;
                return ret;
            } else {
                cerr << endl << "operation fail!" << endl;
                return ret;
            }
        } else if (ret == EC_FALSE) {
            cerr << "path <" << pathName << "> is neither a file or directory" << endl << "operation fail!" << endl;
            return EC_NOTSUP;
        } else {
            cerr << "fail to get meta of path <" << pathName << ">, " << FileSystem::getErrorString(ret) << endl
                 << "operation fail!" << endl;
            return ret;
        }
    } else {
        cerr << "fail to get meta of path <" << pathName << ">, " << FileSystem::getErrorString(ret) << endl
             << "operation fail!" << endl;
        return ret;
    }

    return EC_OK;
}

#undef CONVERT_ZOO_TIME

ErrorCode FsUtil::getZookeeperNodeMeta(const string &pathName, uint32_t &totalNodeCount, uint64_t &size) {
    FileList fileList;
    ErrorCode ret = FileSystem::listDir(pathName, fileList);
    if (ret != EC_OK) {
        cerr << "list dir <" << pathName << "> fail!" << FileSystem::getErrorString(ret) << endl;
        return ret;
    }

    totalNodeCount += fileList.size();
    string subPath;
    for (size_t i = 0; i < fileList.size(); i++) {
        if (!FileSystem::appendPath(pathName, fileList[i], subPath)) {
            cerr << "fail to get subpath. "
                 << "parent path <" << pathName << ">, patname <" << fileList[i] << ">" << endl;
            return EC_PARSEFAIL;
        }

        ret = getZookeeperNodeMeta(subPath, totalNodeCount, size);
        if (ret != EC_OK) {
            return ret;
        }

        FileMeta meta;
        ret = getPathMeta(subPath, meta);
        if (ret != EC_OK) {
            return ret;
        }
        size += meta.fileLength;
    }

    return EC_OK;
}

ErrorCode FsUtil::runFlock(const char *lockname, const char *script) {
    ScopedFileLock lock;
    if (!FileSystem::getScopedFileLock(lockname, lock)) {
        cerr << "get file lock fail! " << endl;
        return EC_UNKNOWN;
    }

    if (0 != system(script)) {
        return EC_UNKNOWN;
    }
    return EC_OK;
}

ErrorCode FsUtil::runForward(const char *command, const char *pathname, const char *args) {
    string output;
    ErrorCode ret = FileSystem::forward(command, pathname, args, output);
    if (ret == EC_OK) {
        cout << (output.empty() ? "OK!" : output) << endl;
    } else if (ret == EC_NOTSUP) {
        cerr << "not suppot command[" << command << "], "
             << "path[" << pathname << "], "
             << "args[" << args << "]" << endl;
    } else {
        cerr << FileSystem::getErrorString(ret) << endl << "operation fail!" << endl;
    }
    return ret;
}

void FsUtil::closeFileSystem() { FileSystem::close(); }

FSLIB_END_NAMESPACE(tools);
