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
#include "build_service/workflow/RealtimeBuilderDefine.h"

#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/DataLinkModeUtil.h"
#include "build_service/config/ResourceReader.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace autil;
using namespace build_service::proto;

namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, RealtimeInfoWrapper);
bool RealtimeInfoWrapper::load(const std::string& indexRoot)
{
    if (indexRoot.empty()) {
        BS_LOG(ERROR, "failed to get path from directory");
        return false;
    }

    const string& generationPath = fslib::util::FileUtil::getParentDir(indexRoot);
    string realtimeInfoFile = fslib::util::FileUtil::joinFilePath(generationPath, REALTIME_INFO_FILE_NAME);
    bool fileExist = false;
    if (!fslib::util::FileUtil::isFile(realtimeInfoFile, fileExist)) {
        BS_LOG(ERROR, "check whether [%s] is file failed.", realtimeInfoFile.c_str());
        return false;
    }
    if (fileExist) {
        string fileContent;
        if (!fslib::util::FileUtil::readFile(realtimeInfoFile, fileContent)) {
            BS_LOG(ERROR, "fail to read %s", realtimeInfoFile.c_str());
            return false;
        }
        KeyValueMap kvMap;
        if (!config::ResourceReader::parseRealtimeInfo(fileContent, kvMap)) {
            BS_LOG(ERROR, "fail to parse realtime info from [%s]", fileContent.c_str());
            return false;
        }
        _kvMap = std::move(kvMap);
        _valid = true;
        BS_LOG(INFO, "parse realtime_info_file [%s], content [%s] success.", realtimeInfoFile.c_str(),
               fileContent.c_str());
    }
    return true;
}

bool RealtimeInfoWrapper::adaptsToDataLinkMode(const std::string& specifiedDataLinkMode)
{
    if (_valid == false) {
        BS_LOG(INFO, "cannot adapts to DataLinkMode[%s] from an invalid state", specifiedDataLinkMode.c_str());
    }
    if (specifiedDataLinkMode.empty()) {
        return true;
    }
    return config::DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(specifiedDataLinkMode, _kvMap);
}

std::string RealtimeInfoWrapper::getBsServerAddress() const
{
    auto iter = _kvMap.find(config::BS_SERVER_ADDRESS);
    if (iter != _kvMap.end()) {
        return iter->second;
    }
    return "";
}

bool RealtimeInfoWrapper::needReadRawDoc() const
{
    if (!_valid) {
        // no realtime info in index root, maybe build from suez directly
        return true;
    }
    std::string mode;
    auto it = _kvMap.find(config::REALTIME_MODE);
    if (it != _kvMap.end()) {
        mode = it->second;
    }
    return mode == config::REALTIME_JOB_MODE || mode == config::REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE;
}

}} // namespace build_service::workflow
