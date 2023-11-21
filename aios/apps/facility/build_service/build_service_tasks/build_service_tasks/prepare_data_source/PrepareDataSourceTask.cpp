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
#include "build_service_tasks/prepare_data_source/PrepareDataSourceTask.h"

#include <map>
#include <memory>
#include <sstream>
#include <stddef.h>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/reader/CollectResult.h"
#include "build_service/reader/FileListCollector.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace autil;
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::reader;
using namespace build_service::proto;
using namespace std;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, PrepareDataSourceTask);

const string PrepareDataSourceTask::TASK_NAME = BS_PREPARE_DATA_SOURCE;
PrepareDataSourceTask::PrepareDataSourceTask() {}

PrepareDataSourceTask::~PrepareDataSourceTask() {}

bool PrepareDataSourceTask::init(TaskInitParam& initParam)
{
    _taskInitParam = initParam;
    return true;
}

bool PrepareDataSourceTask::handleTarget(const config::TaskTarget& target)
{
    if (isDone(target)) {
        return true;
    }
    string fileMetaPath;
    if (!FileListCollector::getBSRawFileMetaDir(_taskInitParam.resourceReader.get(), _taskInitParam.buildId.dataTable,
                                                _taskInitParam.buildId.generationId, fileMetaPath)) {
        BS_LOG(ERROR, "get file meta path failed");
        return false;
    }
    string fileName = fslib::util::FileUtil::joinFilePath(fileMetaPath, BS_RAW_FILES_META);
    bool isExist;
    if (!fslib::util::FileUtil::isFile(fileName, isExist)) {
        BS_LOG(ERROR, "fail to get file meta [%s]", fileName.c_str());
        return false;
    }
    if (isExist) {
        BS_LOG(INFO, "get file meta [%s] is already exist", fileName.c_str());
        ScopedLock lock(_lock);
        _currentFinishTarget = target;
        return true;
    }
    string dataDescriptionKvs;
    vector<DataDescription> dataDescriptions;
    if (!target.getTargetDescription("dataDescriptions", dataDescriptionKvs)) {
        BS_LOG(INFO, "no datadescriptions info, do nothing");
        ScopedLock lock(_lock);
        _currentFinishTarget = target;
        return true;
    }
    try {
        FromJsonString(dataDescriptions, dataDescriptionKvs);
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "parse data descriptions from [" << dataDescriptionKvs << "] failed, exception[" << string(e.what())
           << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    DescriptionToCollectResultsMap descriptionResultMap;
    for (size_t i = 0; i < dataDescriptions.size(); i++) {
        auto description = dataDescriptions[i];
        auto iter = description.find(READ_SRC_TYPE);
        if (iter != description.end() && iter->second == FILE_READ_SRC) {
            KeyValueMap kvMap;
            string descriptionKey = ToJsonString(description);

            for (auto it = description.begin(); it != description.end(); ++it) {
                kvMap[it->first] = it->second;
            }
            CollectResults results;
            proto::Range range;
            range.set_from(0);
            range.set_to(65535);
            if (!FileListCollector::collect(kvMap, range, results)) {
                BS_LOG(ERROR, "get files failed");
                return false;
            }
            descriptionResultMap[descriptionKey] = results;
        }
    }
    try {
        string fileContent = ToJsonString(descriptionResultMap);
        indexlib::file_system::FslibWrapper::AtomicStoreE(fileName, fileContent);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "catch exception %s", e.what());
        return false;
    }
    {
        ScopedLock lock(_lock);
        _currentFinishTarget = target;
    }
    return true;
}

bool PrepareDataSourceTask::isDone(const config::TaskTarget& targetDescription)
{
    ScopedLock lock(_lock);
    if (targetDescription == _currentFinishTarget) {
        return true;
    }
    return false;
}

indexlib::util::CounterMapPtr PrepareDataSourceTask::getCounterMap() { return indexlib::util::CounterMapPtr(); }

std::string PrepareDataSourceTask::getTaskStatus() { return "prepare data source is running"; }

}} // namespace build_service::task_base
