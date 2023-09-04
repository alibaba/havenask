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
#ifndef FSLIB_PLUGIN_HDFS_FILE_H
#define FSLIB_PLUGIN_HDFS_FILE_H

#include "fslib/common.h"
#include "fslib/fs/hdfs/FileMonitor.h"
#include "fslib/fs/hdfs/HdfsConnection.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(hdfs);

class HdfsFile : public File {
public:
    HdfsFile(const std::string &originalPath,
             const std::string &dstPath,
             HdfsConnectionPtr &connection,
             hdfsFile file,
             PanguMonitor *panguMonitor = NULL,
             int64_t curPos = 0,
             ErrorCode ec = EC_OK);
    ~HdfsFile();

    friend class HdfsFileTest;

public:
    ssize_t read(void *buffer, size_t length) override;
    ssize_t write(const void *buffer, size_t length) override;
    ssize_t pread(void *buffer, size_t length, off_t offset) override;
    ssize_t pwrite(const void *buffer, size_t length, off_t offset) override;
    ErrorCode flush() override;
    ErrorCode sync() override;
    ErrorCode close() override;
    ErrorCode seek(int64_t offset, SeekFlag flag) override;
    int64_t tell() override;
    bool isOpened() const override;
    bool isEof() override;

private:
    int64_t getFileLen();

private:
    HdfsConnectionPtr _connection;
    hdfsFile _file;
    int64_t _curPos;
    bool _isEof;
    std::string _dstPath;
    std::unique_ptr<FileMonitor> _fileMonitor;
};

typedef std::unique_ptr<HdfsFile> HdfsFilePtr;

FSLIB_PLUGIN_END_NAMESPACE(hdfs);

#endif // FSLIB_PLUGIN_HDFS_FILE_H
