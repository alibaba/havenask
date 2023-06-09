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
#include <stack>
#include "fslib/cache/CacheFile.h"
#include "fslib/common/common_define.h"
#include "autil/TimeUtility.h"
using namespace std;
using namespace autil;
FSLIB_BEGIN_NAMESPACE(cache);
AUTIL_DECLARE_AND_SETUP_LOGGER(cache, CacheFile);

size_t CacheFile::_alignment = getpagesize();
#define barrier()  __asm__ __volatile__("": : :"memory")

CacheFile::CacheFile(const std::string &fileName, FileBlockCache *cache,
                     int64_t id, size_t blockSize)
{
    _fileName = fileName;
    _id = id;
    _cache = cache;
    _blockSize = blockSize;
    _file = NULL;
    _threadPool = NULL;
    _writeFailedCount = 0;
    _unWrittenLen = 0;
    _actualFileLen = 0;
}

CacheFile::~CacheFile() {
    if (_threadPool != NULL) {
        _threadPool->stop(autil::ThreadPool::STOP_AFTER_QUEUE_EMPTY);
        delete _threadPool;
        _threadPool = NULL;
    }
    if (_file != NULL) {
        delete _file;
    }
}

bool CacheFile::open(fslib::Flag mode, bool useDirectIO, bool isAsync) {
    if (_file != NULL) {
        AUTIL_LOG(ERROR, "file should not be open twice! fileName[%s].",
                  _fileName.c_str());
        return false;
    }
    if (useDirectIO && _blockSize % _alignment != 0) {
        AUTIL_LOG(ERROR, "blockSize should aligned by device block size!");
        return false;
    }

    _file = fs::FileSystem::openFile(_fileName, mode, useDirectIO);

    if (_file == NULL) {
        AUTIL_LOG(ERROR, "open file failed! fileName:[%s]", _fileName.c_str());

        return false;
    }

    if (_file->getLastError() != fslib::EC_OK) {
        return false;
    }
    if (isAsync && (mode == fslib::APPEND || mode == fslib::WRITE)) {
        _threadPool = new ThreadPool(1);
        if (!_threadPool->start("fslibCacheF")) {
            AUTIL_LOG(ERROR, "start threadPool failed!");
            return false;
        }
    }

    _writeFailedCount = 0;
    _unWrittenLen = 0;

    _actualFileLen = tell();

    return true;
}

ssize_t CacheFile::read(void *buffer, size_t len) {
    size_t fromCacheSize;
    return read(buffer, len, fromCacheSize);
}

ssize_t CacheFile::read(void *buffer, size_t len, size_t &fromCacheSize) {
    fromCacheSize = 0;
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "should open file first! fileName:[%s]",
                  _fileName.c_str());
        return -1;
    }
    autil::ScopedLock lock(_mutex);
    int64_t offset = _file->tell();
    if (offset < 0) {
        AUTIL_LOG(ERROR, "get offset failed! fileName:[%s]",
                  _fileName.c_str());
        return -1;
    }
    ssize_t ret = pread(buffer, len, offset, fromCacheSize);
    if (ret != -1) {
        seek(ret, FILE_SEEK_CUR);
    }
    return ret;
}

ssize_t CacheFile::pread(void *buffer, size_t len, int64_t offset) {
    size_t fromCacheSize;
    return pread(buffer, len, offset, fromCacheSize);
}

ssize_t CacheFile::pread(void *buffer, size_t len, int64_t offset, size_t &fromCacheSize) {
    fromCacheSize = 0;
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "should open file first! fileName:[%s]",
                  _fileName.c_str());
        return -1;
    }
    assert(_blockSize > 0);
    ssize_t totalReadLen = 0;
    while (len > 0) {
        bool fromCache = false;
        FileBlockPtr block = getOneBlock(offset / _blockSize * _blockSize, fromCache);
        if (block == NULL) {
            AUTIL_LOG(ERROR, "read block failed, fileName:[%s], offset:[%ld]",
                      _fileName.c_str(), offset);
            return -1;
        }
        int64_t offsetDelta = offset % _blockSize;
        if (offsetDelta >= (int64_t)block->getActualLen()) {
            break;
        }
        ssize_t readLen = min(block->getActualLen() - offsetDelta, len);
        if (fromCache) {
            fromCacheSize += readLen;
        }
        memcpy((char*)buffer + totalReadLen,
               (char*)block->getBuffer() + offsetDelta, readLen);
        totalReadLen += readLen;
        offset += readLen;
        len -= readLen;
    }
    return totalReadLen;
}

FileBlockPtr CacheFile::getOneBlock(int64_t offset, bool &fromCache) {
    fromCache = false;
    FileBlockPtr fileBlock = getBlockFromCache(offset);
    if (fileBlock != NULL) {
        fromCache = true;
        return fileBlock;
    }
    autil::ScopedLock lock(_blockMutex);

    // check after acquire lock. Block may have been read by other thread
    fileBlock = getBlockFromCache(offset);
    if (fileBlock != NULL) {
        fromCache = true;
        return fileBlock;
    }
    fileBlock.reset(new FileBlock(offset, _blockSize));
    FileBlockPool *pool = NULL;
    if (_cache != NULL) {
        pool = _cache->getFileBlockPool();
    }
    fileBlock->init(pool);

    ssize_t readLen = _file->pread(fileBlock->getBuffer(),  _blockSize, offset);
    if (readLen < 0) {
        AUTIL_LOG(ERROR, "file pread failed");
        return FileBlockPtr();
    }

    fileBlock->setActualLen(readLen);

    if ((size_t)readLen == _blockSize){
        putBlockToCache(offset, fileBlock);
    }

    return fileBlock;
}

FileBlockPtr CacheFile::getBlockFromCache(int64_t offset) {
    CacheKey cacheKey = make_pair(make_pair(_fileName, _id),
                                  make_pair(offset, _blockSize));
    FileBlockPtr fileBlock;
    if (_cache != NULL) {
        _cache->get(cacheKey, fileBlock);
    }
    return fileBlock;
}

void CacheFile::putBlockToCache(int64_t offset, FileBlockPtr fileBlock) {
    CacheKey cacheKey = make_pair(make_pair(_fileName, _id),
                                  make_pair(offset, _blockSize));
    if (_cache != NULL) {
        _cache->put(cacheKey, fileBlock);
    }
}

void CacheFile::putBufferToCache(
        FileBlockPtr fileBlock, int64_t curOff, size_t len)
{
    if (_cache == NULL) {
        return ;
    }

    if (len < _blockSize) {
        return ;
    }

    if (getBlockFromCache(curOff ) == NULL) {
        putBlockToCache(curOff, fileBlock);
    }
}

ssize_t CacheFile::write(const void *buffer, size_t len, int64_t offset) {
    assert(len <= MAX_UNWRITTEN_LEN);
    while (_unWrittenLen.load() + len > MAX_UNWRITTEN_LEN) {
        usleep(10 * 1000);
    }
    _unWrittenLen += len;
    _unWrittenLenSnapshot = _unWrittenLen.load();
    ssize_t ret = len;
    while (len > 0) {
        size_t sz = len > _blockSize ? _blockSize : len;
        if (writeSlice(buffer, sz, offset) != (ssize_t)sz) {
            barrier();
            _unWrittenLen -= len;
            return -1;
        }
        buffer = (const void *)((const char *)buffer + sz);
        len -= sz;
        if (offset != -1) {
            offset += sz;
        }
    }
    return ret;
}

ssize_t CacheFile::writeSlice(const void *buffer, size_t len, int64_t offset) {
    assert(len <= _blockSize);
    ssize_t ret = len;

    FileBlockPtr fileBlock(new FileBlock(offset, _blockSize));
    FileBlockPool *pool = NULL;
    if (_cache != NULL) {
        pool = _cache->getFileBlockPool();
    }
    fileBlock->init(pool);
    memcpy(fileBlock->getBuffer(), (char*)buffer, len);
    fileBlock->setActualLen(len);

    autil::ScopedLock lock(_mutex);
    if (_threadPool != NULL) {
        //doWrite will free alignedBuffer
        auto task = [this, fileBlock, len, offset]() {
                        doWriteAuto(fileBlock, len, offset);
                    };
        if (ThreadPool::ERROR_NONE != _threadPool->pushTask(std::move(task))) {
            AUTIL_LOG(ERROR, "fail to push write work item.");
            return -1;
        }
    } else {
        ret = doWriteSafe(fileBlock, len, offset);
    }

    if (ret < 0) {
        AUTIL_LOG(ERROR, "file write failed, fileName:[%s]", _fileName.c_str());
    }
    return ret;
}

ssize_t CacheFile::write(const void *buffer, size_t len) {
    if (len > MAX_UNWRITTEN_LEN) {
        AUTIL_LOG(ERROR, "Do not support write more than %ld byte in ont time! "
                  "fileName:[%s]", MAX_UNWRITTEN_LEN, _fileName.c_str());
        return -1;
    }
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "should open file first! fileName:[%s]",
                  _fileName.c_str());
        return -1;
    }

    return write(buffer, len, -1);
}

ssize_t CacheFile::doWriteAuto(FileBlockPtr fileBlock, size_t len, int64_t offset) {
    ssize_t ret = doWriteSafe(fileBlock, len, offset);
    if (ret == -1) {
        assert(_unWrittenLen.load() >= (int64_t)len);
        barrier();
        _unWrittenLen -= len;
    }
    return ret;
}

ssize_t CacheFile::doWriteSafe(FileBlockPtr fileBlock, size_t len, int64_t offset) {
    ssize_t ret = doWrite(fileBlock, len, offset);
    if (ret != -1) {
        assert(_unWrittenLen.load() >= (int64_t)len);
        barrier();
        _unWrittenLen -= len;
        _actualFileLen -= ret;
    }
    return ret;
}

ssize_t CacheFile::doWrite(FileBlockPtr fileBlock, size_t len, int64_t offset) {
    ssize_t ret = len;
    if (_writeFailedCount > 0) {
        AUTIL_LOG(ERROR, "file broken! fileName:[%s]",
                  _fileName.c_str());
        return -1;
    }
    if (offset == -1) {
        offset = _file->tell();
        if (offset < 0) {
            AUTIL_LOG(ERROR, "get offset failed! fileName:[%s]",
                      _fileName.c_str());
            _writeFailedCount ++;
            return -1;
        }
        ssize_t more = (ssize_t) len;
        int64_t off = 0;
        while (more > 0) {
            ssize_t writeLen = _file->write(
                    (char*)fileBlock->getBuffer() + off, more);
            if (writeLen == -1) {
                ret = -1;
                break;
            }
            more -= writeLen;
            off += writeLen;
            ret = off;
        }
    } else {
        ret = _file->pwrite(fileBlock->getBuffer(), len, offset);
    }

    if (ret == -1) {
        AUTIL_LOG(ERROR, "do write failed! fileName:[%s]",
                  _fileName.c_str());
        _writeFailedCount ++;
    } else {
        // put buffer to cache, do not free
        putBufferToCache(fileBlock, offset, ret);
    }

    return ret;
}

ssize_t CacheFile::pwrite(const void *buffer, size_t len, int64_t offset)
{
    if (len > MAX_UNWRITTEN_LEN) {
        AUTIL_LOG(ERROR, "Do not support write more than %ld byte in ont time! "
                  "fileName:[%s]", MAX_UNWRITTEN_LEN, _fileName.c_str());
        return -1;
    }
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "should open file first! fileName:[%s]",
                  _fileName.c_str());
        return -1;
    }

    return write(buffer, len, offset);
}

ErrorCode CacheFile::close() {
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "should open file first! fileName:[%s]",
                  _fileName.c_str());
        return EC_BADF;
    }
    if (_threadPool != NULL) {
        _threadPool->stop(autil::ThreadPool::STOP_AFTER_QUEUE_EMPTY);
        delete _threadPool;
        _threadPool = NULL;
    }
    ErrorCode ec = _file->close();
    if (ec == EC_OK) {
        delete _file;
        _file = NULL;
    }
    _unWrittenLen = 0;
    _writeFailedCount = 0;
    return ec;
}

ErrorCode CacheFile::flush() {
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "should open file first! fileName:[%s]",
                  _fileName.c_str());
        return EC_BADF;
    }
    autil::ScopedLock lock(_mutex);
    if (_threadPool != NULL) {
        while (_unWrittenLen.load() > 0) {
            usleep(10 * 1000);
        }
    }
    ErrorCode ec = _file->flush();
    if (ec != EC_OK) {
        return ec;
    }
    if (_writeFailedCount > 0) {
        AUTIL_LOG(ERROR, "write failed %d times in fileName:[%s]",
                  _writeFailedCount, _fileName.c_str());
        return EC_UNKNOWN;
    }
    return EC_OK;
}

ErrorCode CacheFile::sync() {
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "should open file first! fileName:[%s]",
                  _fileName.c_str());
        return EC_BADF;
    }

    ErrorCode ec = _file->sync();
    if (ec != EC_OK) {
        return ec;
    }

    return EC_OK;
}

ErrorCode CacheFile::seek(int64_t offset, SeekFlag flag) {
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "should open file first! fileName:[%s]",
                  _fileName.c_str());
        return EC_BADF;
    }
    return _file->seek(offset, flag);
}

int64_t CacheFile::tell() {
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "should open file first! fileName:[%s]",
                  _fileName.c_str());
        return -1;
    }
    if (_unWrittenLen.load() > 0) {
        flush();
    }
    return _file->tell();
}

bool CacheFile::isOpened() const {
    return _file != NULL && _file->isOpened();
}

bool CacheFile::isEof() {
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "should open file first! fileName:[%s]",
                  _fileName.c_str());
        return false;
    }
    return _file->isEof();
}

FSLIB_END_NAMESPACE(cache);
