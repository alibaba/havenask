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
#ifndef FSLIB_PROXYFILE_H
#define FSLIB_PROXYFILE_H

#include <cstdarg>

#include "autil/Log.h"
#include "autil/legacy/md5.h"
#include "fslib/fslib.h"
#include "fslib/fs/File.h"
#include "fslib/fs/SpeedController.h"

FSLIB_BEGIN_NAMESPACE(fs);

class ProxyFile : public File
{
public:
    ProxyFile(const std::string& filePath, File* file, Flag flag, 
              SpeedController* controller);
    ~ProxyFile();
    
public:
    ssize_t read(void* buffer, size_t length) override;
    ssize_t write(const void* buffer, size_t length) override;
    ssize_t pread(void* buffer, size_t length, off_t offset) override;

#if (__cplusplus >= 201703L)
    void pread(IOController* controller, void* buffer, size_t length, off_t offset,
               std::function<void()> callback) override;
#endif
    ssize_t preadv(const iovec* iov, int iovcnt, off_t offset) override;

    #if (__cplusplus >= 201703L)
    void preadv(IOController* controller, const iovec* iov, int iovcnt, off_t offset,
                std::function<void()> callback) override;
    #endif
    ssize_t pwrite(const void* buffer, size_t length, off_t offset) override;

#if (__cplusplus >= 201703L)
    void pwrite(IOController* controller, void* buffer, size_t length, off_t offset,
               std::function<void()> callback) override;

    void pwritev(IOController* controller, const iovec* iov, int iovcnt,
                 off_t offset, std::function<void()> callback) override;
#endif
    ErrorCode getLastError() const override;
    ErrorCode flush() override;
    ErrorCode sync() override;
    ErrorCode close() override;
    ErrorCode seek(int64_t offset, SeekFlag flag) override;
    int64_t tell() override;
    bool isOpened() const override;
    bool isEof() override;

    ErrorCode ioctlImpl(IOCtlRequest request, va_list args) override;

private:
    std::string GetMD5String() const;
    std::string GetIp() const;
    std::string GetHostName() const;
    
private:
    typedef autil::legacy::Md5Stream Md5Stream;
    FSLIB_TYPEDEF_AUTO_PTR(Md5Stream);
    
    File* _file;
    std::string _filePath;
    Flag _flag;
    Md5StreamPtr _md5Stream;
    size_t _writeLen;
    SpeedController* _controller;
};

FSLIB_TYPEDEF_AUTO_PTR(ProxyFile);

FSLIB_END_NAMESPACE(fs);

#endif //FSLIB_PROXYFILE_H
