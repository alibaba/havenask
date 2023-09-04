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
#include "swift/util/TargetRecorder.h"

#include <algorithm>
#include <cstddef>
#include <limits.h>
#include <unistd.h>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"

using namespace fslib::fs;
using namespace std;
namespace swift {
namespace util {

size_t TargetRecorder::_maxCount = 50;

TargetRecorder::TargetRecorder() {}

TargetRecorder::~TargetRecorder() {}

void TargetRecorder::newAdminTopic(const std::string &target) { newRecord(target, "admin_topic", "topic"); }

void TargetRecorder::newAdminPartition(const std::string &target) { newRecord(target, "admin_partition", "partition"); }

void TargetRecorder::newRecord(const std::string &content, const std::string &dirname, const std::string &suffix) {
    string cwd;
    if (!TargetRecorder::getCurrentPath(cwd)) {
        return;
    }
    string dir = FileSystem::joinFilePath(cwd, dirname);
    if (fslib::EC_TRUE != FileSystem::isExist(dir)) {
        if (fslib::EC_OK != FileSystem::mkDir(dir)) {
            return;
        }
    }

    vector<string> allfiles;
    if (fslib::EC_OK == FileSystem::listDir(dir, allfiles)) {
        vector<string> records;
        for (size_t i = 0; i < allfiles.size(); i++) {
            if (hasSuffix(allfiles[i], suffix)) {
                records.push_back(allfiles[i]);
            }
        }
        if (records.size() > _maxCount) {
            sort(records.begin(), records.end());
            for (size_t i = 0; i < records.size() - _maxCount; i++) {
                string removeFile = FileSystem::joinFilePath(dir, records[i]);
                FileSystem::remove(removeFile);
            }
        }
    }
    string filename = autil::TimeUtility::currentTimeString("%Y-%m-%d-%H-%M-%S") + "_" + suffix;
    string path = FileSystem::joinFilePath(dir, filename);
    string formatContent = content;
    try {
        autil::legacy::Any a = autil::legacy::json::ParseJson(content);
        formatContent = autil::legacy::json::ToString(a);
    } catch (const autil::legacy::ExceptionBase &e) {}
    FileSystem::writeFile(path, formatContent);
}

bool TargetRecorder::hasSuffix(const std::string &str, const std::string &suffix) {
    return str.size() >= suffix.size() && str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool TargetRecorder::getCurrentPath(std::string &path) {
    char cwdPath[PATH_MAX];
    char *ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        return false;
    }
    path = string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }
    return true;
}

void TargetRecorder::setMaxFileCount(size_t count) { _maxCount = count; }
} // namespace util
} // namespace swift
