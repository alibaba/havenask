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
#include "common/Recorder.h"
#include "common/PathUtil.h"
#include "autil/legacy/jsonizable.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"

using namespace fslib::fs;
using namespace std;

BEGIN_CARBON_NAMESPACE(common);

CARBON_LOG_SETUP(common, Recorder);

size_t Recorder::_maxCount = RECORD_MAX_COUNT;
string Recorder::_basePath = CARBON_PLAN_LOCAL_RECORD_PATH;
map<string, string> Recorder::_cache;
autil::ThreadMutex Recorder::_mutex;

Recorder::Recorder() {
}

Recorder::~Recorder() {
}

void Recorder::newTarget(const string &UID, const string &target) {
    newRecord(target, UID, "target");
}

void Recorder::newFinalTarget(const string &UID, const string &finalTarget) {
    newRecord(finalTarget, UID, "final");
}

void Recorder::newCurrent(const string &UID, const string &current) {
    newRecord(current, UID, "current");
}

void Recorder::newRecord(const string &content, const string &dirname, const string &suffix) {
    string cwd;
    if (!PathUtil::getCurrentPath(cwd)) {
        return;
    }
    string dir = FileSystem::joinFilePath(
            FileSystem::joinFilePath(cwd, _basePath), dirname);
    if (fslib::EC_TRUE != FileSystem::isExist(dir)) {
        if (fslib::EC_OK != FileSystem::mkDir(dir, true)) {
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
    } catch (const autil::legacy::ExceptionBase &e) {
    }
    FileSystem::writeFile(path, formatContent);
}

bool Recorder::hasSuffix(const string &str, const string &suffix) {
    return str.size() >= suffix.size()
        && str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

void Recorder::logWhenDiff(const string &key,
                           const string &content,
                           alog::Logger *logger)
{
    autil::ScopedLock lock(_mutex);
    if (_cache[key] != content) {
        ALOG_LOG(logger, alog::LOG_LEVEL_INFO, "%s:%s", key.c_str(), content.c_str());
        _cache[key] = content;
    }
}

bool Recorder::diff(const string &key, const string &content) {
    autil::ScopedLock lock(_mutex);
    if (_cache[key] != content) {
        _cache[key] = content;
        return true;
    }
    return false;
}

END_CARBON_NAMESPACE(common);
