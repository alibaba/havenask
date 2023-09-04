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
#include "indexlib/util/metrics/IndexlibMetricControl.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fslib.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, IndexlibMetricControl);

IndexlibMetricControl::IndexlibMetricControl() { InitFromEnv(); }

IndexlibMetricControl::~IndexlibMetricControl() {}

IndexlibMetricControl::Status IndexlibMetricControl::Get(const string& metricName) const noexcept
{
    Status status;
    for (size_t i = 0; i < _statusItems.size(); i++) {
        if (_statusItems[i].first->Match(metricName)) {
            status = _statusItems[i].second;
            break;
        }
    }
    AUTIL_LOG(DEBUG, "metricName [%s] use gatherId [%s], prohibit [%s]", metricName.c_str(), status.gatherId.c_str(),
              status.prohibit ? "true" : "false");
    return status;
}

void IndexlibMetricControl::InitFromEnv()
{
    string envParam = autil::EnvUtil::getEnv("INDEXLIB_METRIC_PARAM");
    if (!envParam.empty()) {
        AUTIL_LOG(INFO, "Init IndexlibMetricControl by env INDEXLIB_METRIC_PARAM!");
        InitFromString(envParam);
        return;
    }

    string filePath = autil::EnvUtil::getEnv("INDEXLIB_METRIC_PARAM_FILE_PATH");
    if (!filePath.empty()) {
        AUTIL_LOG(INFO,
                  "Init IndexlibMetricControl by env "
                  "INDEXLIB_METRIC_PARAM_FILE_PATH [%s]!",
                  filePath.c_str());
        if (!IsExist(filePath)) {
            AUTIL_LOG(INFO, "INDEXLIB_METRIC_PARAM_FILE_PATH [%s] not exist!", filePath.c_str());
            return;
        }
        string fileContent;
        try {
            AtomicLoad(filePath, fileContent);
        } catch (...) {
            AUTIL_LOG(INFO, "load INDEXLIB_METRIC_PARAM_FILE_PATH [%s] catch exception!", filePath.c_str());
            return;
        }
        InitFromString(string(fileContent));
        return;
    }
}

void IndexlibMetricControl::InitFromString(const string& paramStr)
{
    if (!_statusItems.empty()) {
        AUTIL_LOG(INFO, "IndexlibMetricControl has already been inited!");
        return;
    }

    AUTIL_LOG(INFO, "Init IndexlibMetricControl with param [%s]!", paramStr.c_str());
    vector<PatternItem> patternItems;
    ParsePatternItem(patternItems, paramStr);
    for (size_t i = 0; i < patternItems.size(); i++) {
        if (patternItems[i].patternStr.empty()) {
            AUTIL_LOG(ERROR, "pattern should not be empty!");
            return;
        }
        util::RegularExpressionPtr regExpr(new util::RegularExpression);
        if (!regExpr->Init(patternItems[i].patternStr)) {
            AUTIL_LOG(ERROR, "Init RegularExpression fail with pattern [%s], ERROR [%s]!",
                      patternItems[i].patternStr.c_str(), regExpr->GetLatestErrMessage().c_str());
            return;
        }
        _statusItems.push_back(make_pair(regExpr, Status(patternItems[i].gatherId, patternItems[i].prohibit)));
    }
}

void IndexlibMetricControl::ParsePatternItem(vector<PatternItem>& patternItems, const string& paramStr)
{
    string trimStr = paramStr;
    StringUtil::trim(trimStr);
    if (trimStr[0] == '[') {
        ParseJsonString(patternItems, trimStr);
    } else {
        ParseFlatString(patternItems, trimStr);
    }
}

void IndexlibMetricControl::ParseFlatString(vector<PatternItem>& patternItems, const string& paramStr)
{
    vector<string> paramVec;
    StringUtil::fromString(paramStr, paramVec, ";");
    for (size_t i = 0; i < paramVec.size(); i++) {
        vector<vector<string>> patternStrVec;
        StringUtil::fromString(paramVec[i], patternStrVec, "=", ",");
        if (patternStrVec.size() > 3) {
            AUTIL_LOG(ERROR,
                      "INDEXLIB_METRIC_PARAM [%s] has format error:"
                      "too many keys [%s]!",
                      paramStr.c_str(), paramVec[i].c_str());
            return;
        }

        PatternItem patternItem;
        for (size_t j = 0; j < patternStrVec.size(); j++) {
            if (patternStrVec[j].size() != 2) {
                AUTIL_LOG(ERROR,
                          "INDEXLIB_METRIC_PARAM [%s] has format error:"
                          "inner should be kv pair format!",
                          paramStr.c_str());
                return;
            }
            if (patternStrVec[j][0] == "pattern") {
                patternItem.patternStr = patternStrVec[j][1];
                continue;
            }
            if (patternStrVec[j][0] == "id") {
                patternItem.gatherId = patternStrVec[j][1];
                continue;
            }
            if (patternStrVec[j][0] == "prohibit") {
                patternItem.prohibit = (patternStrVec[j][1] == "true") ? true : false;
                continue;
            }
            AUTIL_LOG(WARN, "unknown key [%s] in INDEXLIB_METRIC_PARAM [%s]!", patternStrVec[j][0].c_str(),
                      paramStr.c_str());
        }
        patternItems.push_back(patternItem);
    }
}

void IndexlibMetricControl::ParseJsonString(vector<PatternItem>& patternItems, const string& paramStr)
{
    try {
        vector<PatternItem> tmp;
        autil::legacy::FromJsonString(patternItems, paramStr);
        patternItems.insert(patternItems.end(), tmp.begin(), tmp.end());
    } catch (const exception& e) {
        AUTIL_LOG(ERROR, "parse json format [%s] catch exception: %s", paramStr.c_str(), e.what());
        return;
    } catch (...) {
        AUTIL_LOG(ERROR, "parse json format [%s] catch exception", paramStr.c_str());
    }
}

// duplicate code : avoid dependency to storage
bool IndexlibMetricControl::IsExist(const string& filePath)
{
    ErrorCode ec = FileSystem::isExist(filePath);
    if (ec == EC_TRUE) {
        return true;
    } else if (ec == EC_FALSE) {
        return false;
    }

    INDEXLIB_FATAL_ERROR(FileIO,
                         "Determine existence of file [%s] "
                         "FAILED: [%s]",
                         filePath.c_str(), FileSystem::getErrorString(ec).c_str());
    return false;
}

void IndexlibMetricControl::AtomicLoad(const string& filePath, string& fileContent)
{
    FileMeta fileMeta;
    fileMeta.fileLength = 0;
    ErrorCode ec = FileSystem::getFileMeta(filePath, fileMeta);
    if (ec == EC_NOENT) {
        INDEXLIB_FATAL_ERROR(NonExist, "File [%s] not exist.", filePath.c_str());
    } else if (ec != EC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "[%s] Get file : [%s] meta FAILED", FileSystem::getErrorString(ec).c_str(),
                             filePath.c_str());
    }

    FilePtr file(FileSystem::openFile(filePath, READ, false, fileMeta.fileLength));
    if (unlikely(!file)) {
        INDEXLIB_FATAL_ERROR(FileIO, "Open file[%s] FAILED", filePath.c_str());
    }
    if (!file->isOpened()) {
        ErrorCode ec = file->getLastError();
        if (ec == EC_NOENT) {
            INDEXLIB_FATAL_ERROR(NonExist, "file not exist [%s].", filePath.c_str());
        } else {
            INDEXLIB_FATAL_ERROR(FileIO, "Open file[%s] FAILED, error: [%s]", filePath.c_str(),
                                 FileSystem::getErrorString(ec).c_str());
        }
    }

    fileContent.resize(fileMeta.fileLength);
    char* data = const_cast<char*>(fileContent.c_str());
    ssize_t readBytes = file->read(data, fileMeta.fileLength);
    if (readBytes != (ssize_t)fileMeta.fileLength) {
        INDEXLIB_FATAL_ERROR(FileIO, "Read file[%s] FAILED, error: [%s]", filePath.c_str(),
                             FileSystem::getErrorString(file->getLastError()).c_str());
    }

    ErrorCode ret = file->close();
    if (ret != EC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "Close file[%s] FAILED, error: [%s]", filePath.c_str(),
                             FileSystem::getErrorString(ret).c_str());
    }
}
}} // namespace indexlib::util
