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
#ifndef ISEARCH_ACCESS_LOG_LOCALACCESSLOGREADER_H
#define ISEARCH_ACCESS_LOG_LOCALACCESSLOGREADER_H

#include <limits>

#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "access_log/common.h"
#include "access_log/AccessLogReader.h"

namespace access_log {

class LocalAccessLogReader : public AccessLogReader {
 public:
     LocalAccessLogReader(const std::string &logName, const std::string &logDir = "");
     LocalAccessLogReader(const std::string &logName, std::vector<fslib::fs::File *> logHandlers);
     ~LocalAccessLogReader();

 private:
    LocalAccessLogReader(const LocalAccessLogReader &) = delete;
    LocalAccessLogReader &operator=(const LocalAccessLogReader &) = delete;

 public:
    bool init() override;

    bool read(uint32_t count, std::vector<PbAccessLog> &accessLogs,
              int32_t statusCode = std::numeric_limits<int32_t>::min()) override;

    bool readCustomLog(uint32_t count, std::vector<std::string> &customLogs) override;

    std::string getType() const override { return LOCAL_TYPE; }
    // 读取log 原始数据
    static bool readRawLog(fslib::fs::File *curHandler, std::string &rawStr);
    static bool readOneLog(fslib::fs::File *curHandler, PbAccessLog &pbLog);

    void getLocalFullPathFileNames(const std::string &logFileNamePrefix,
                                   fslib::FileList &fileList);

 private:
    bool initLogHandler();

    void releaseResource() override;

    fslib::fs::File *getCurHandler();

    // virtual for test
    virtual void getLocalFileNames(const std::string &logFileNamePrefix,
                                   fslib::FileList &fileList);
 private:
    std::string _logDir;
    std::string _logFileNamePrefix;
    uint32_t _fileOffset;
    std::vector<fslib::fs::File *> _logHanlers;
};
}

#endif  // ISEARCH_ACCESS_LOG_LOCALACCESSLOGREADER_H
