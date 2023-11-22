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

#include <map>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "swift/common/Common.h"
#include "swift/common/FilePair.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace util {

class FileManagerUtil {
public:
    using FileLists = std::vector<std::pair<std::string, std::string>>;
    using MetaDataMap = std::map<std::pair<std::string, std::string>, std::pair<std::string, std::string>>;

public:
    FileManagerUtil();
    ~FileManagerUtil();
    FileManagerUtil(const FileManagerUtil &) = delete;
    FileManagerUtil &operator=(const FileManagerUtil &) = delete;

public:
    static fslib::ErrorCode listFiles(const fslib::FileList &filePaths, FileLists &fileList);
    static protocol::ErrorCode initFilePairVec(const FileLists &fileList, storage::FilePairVec &filePairVec);
    static bool removeFilePair(const storage::FilePairPtr &filePair);
    static bool removeFilePair(const std::string &metaFileName, const std::string &dataFileName);
    static bool removeFilePairWithSize(const storage::FilePairPtr &filePair, int64_t &fileSize);
    static std::string generateFilePrefix(int64_t msgId, int64_t timestamp, bool isNew = true);

private:
    static MetaDataMap generateMetaDataPairMap(const FileLists &fileList);
    static bool extractMessageIdAndTimestamp(const std::string &prefix, int64_t &msgId, int64_t &timestamp);
    static int64_t getFileSize(const std::string &fileName);
    static bool removeFile(const std::string &fileName);

public:
    static const std::string DATA_SUFFIX;
    static const std::string META_SUFFIX;
    static const std::string INLINE_FILE;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FileManagerUtil);

} // end namespace util
} // end namespace swift
