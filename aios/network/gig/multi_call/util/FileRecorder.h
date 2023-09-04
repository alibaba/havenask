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
#ifndef ISEARCH_MULTI_CALL_FILERECORDER_H
#define ISEARCH_MULTI_CALL_FILERECORDER_H

#include <string>

#include "aios/network/gig/multi_call/common/common.h"
namespace multi_call {

class FileRecorder
{
public:
    FileRecorder();
    ~FileRecorder();

private:
    FileRecorder(const FileRecorder &);
    FileRecorder &operator=(const FileRecorder &);

public:
    static void recordSnapshot(const std::string &content, size_t logCount,
                               const std::string &prefix = "snapshot");
    static void newRecord(const std::string &content, size_t logCount, const std::string &dir,
                          const std::string &suffix);
    static bool hasSuffix(const std::string &str, const std::string &suffix);

private:
    static std::string join(const std::string &path, const std::string &file);
    static bool listDir(const std::string &dirName, std::vector<std::string> &fileList);
    static bool removeFile(const std::string &path);
    static bool writeFile(const std::string &srcFileName, const std::string &content);
    static bool getCurrentPath(std::string &path);
    static bool checkAndCreateDir(const std::string &dir);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_FILERECORDER_H
