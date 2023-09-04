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
#ifndef FSLIB_CACHEFILE_H
#define FSLIB_CACHEFILE_H

#include <atomic>
#include <deque>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "fslib/cache/FifoCache.h"
#include "fslib/cache/FileBlock.h"
#include "fslib/cache/FileBlockPool.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fslib.h"

FSLIB_BEGIN_NAMESPACE(cache);
typedef std::pair<std::pair<std::string, int64_t>, std::pair<int64_t, size_t>> CacheKey;
typedef std::deque<FileBlockPtr> PreReadWindow;
typedef FifoCache<CacheKey, FileBlockPtr, GetFileBlockSize> FileBlockCache;

class CacheFile {
public:
    /****************************************************************************
      you should confirm that the file with same fileName are same one.
      for example: delete a old file and generate a new file with same fileName,
      when read the new file, it may use the old file's block in cache.
      so if you want to delete a file, you should invalidate it's blocks in cache,
      or use id to identify them. and you'd better use absPath as fileName.
    *****************************************************************************/

    CacheFile(const std::string &fileName,
              FileBlockCache *cache,
              int64_t id = 0,
              size_t blockSize = DEFAULT_FILE_BLOCK_SIZE);
    virtual ~CacheFile();

public:
    virtual bool open(fslib::Flag mode, bool useDirectIO = true, bool isAsync = true);
    virtual ssize_t read(void *buffer, size_t len);
    ssize_t read(void *buffer, size_t len, size_t &fromCacheSize);
    virtual ssize_t write(const void *buffer, size_t len);
    virtual ssize_t pread(void *buffer, size_t len, int64_t offset);
    ssize_t pread(void *buffer, size_t len, int64_t offset, size_t &fromCacheSize);
    virtual ssize_t pwrite(const void *buffer, size_t len, int64_t offset);
    virtual ErrorCode flush();
    virtual ErrorCode sync();
    virtual ErrorCode close();
    virtual ErrorCode seek(int64_t offset, SeekFlag flag);
    virtual int64_t tell();
    virtual bool isOpened() const;
    virtual bool isEof();
    virtual ErrorCode getLastError() const { return _file->getLastError(); }
    virtual const char *getFileName() const { return _fileName.c_str(); }
    int64_t getActualFileLen() const { return _actualFileLen.load(); }

private:
    FileBlockPtr getOneBlock(int64_t offset, bool &fromCache);
    FileBlockPtr getBlockFromCache(int64_t offset);
    void putBlockToCache(int64_t offset, FileBlockPtr fileBlock);
    void putBufferToCache(FileBlockPtr fileBLock, int64_t curOff, size_t len);
    ssize_t write(const void *buffer, size_t len, int64_t offset);
    ssize_t writeSlice(const void *buffer, size_t len, int64_t offset);
    ssize_t doWriteAuto(FileBlockPtr fileBlock, size_t len, int64_t offset);
    ssize_t doWriteSafe(FileBlockPtr fileBlock, size_t len, int64_t offset);
    ssize_t doWrite(FileBlockPtr fileBlock, size_t len, int64_t offset);

private:
    friend class CacheFileTest;

private:
    fs::File *_file;
    std::string _fileName;
    FileBlockCache *_cache;
    size_t _blockSize;
    static size_t _alignment;
    autil::ThreadMutex _mutex;
    autil::ThreadMutex _blockMutex;
    int64_t _id;
    autil::ThreadPool *_threadPool;
    int32_t _writeFailedCount;
    std::atomic_int64_t _unWrittenLen;
    std::atomic_int64_t _unWrittenLenSnapshot;
    std::atomic_int64_t _actualFileLen;
};

FSLIB_TYPEDEF_AUTO_PTR(CacheFile);

FSLIB_END_NAMESPACE(cache);

#endif // FSLIB_CACHEFILE_H
