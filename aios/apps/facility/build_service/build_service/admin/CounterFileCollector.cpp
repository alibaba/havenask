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
#include "build_service/admin/CounterFileCollector.h"

#include "autil/TimeUtility.h"
#include "build_service/common/CounterFileSynchronizer.h"
#include "build_service/common/CounterSynchronizer.h"
#include "fslib/fslib.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/util/counter/Counter.h"

using namespace std;
using namespace autil;
using namespace build_service::common;
using namespace build_service::proto;
using namespace build_service::util;
using namespace fslib::fs;
using namespace fslib;
using namespace indexlib::util;
namespace build_service { namespace admin {
BS_LOG_SETUP(admin, CounterFileCollector);

CounterFileCollector::CounterFileCollector() {}

CounterFileCollector::~CounterFileCollector() {}

bool CounterFileCollector::init(const std::string& zkRoot, const proto::BuildId& buildId)
{
    if (zkRoot.empty()) {
        BS_LOG(ERROR, "counter zkRoot is empty");
        return false;
    }
    _buildId = buildId;
    _zkCounterPath = common::CounterSynchronizer::getCounterZkRoot(zkRoot, _buildId);
    return true;
}

bool CounterFileCollector::clearCounters()
{
    if (!fslib::util::FileUtil::remove(_zkCounterPath)) {
        BS_LOG(ERROR, "clear counter dir [%s] failed", _zkCounterPath.c_str());
        return false;
    }
    return true;
}

bool CounterFileCollector::doSyncCounters(RoleCounterMap& roleCounterMap)
{
    autil::ScopedLock lock(_lock);
    FileList fileList;
    ErrorCode ret = FileSystem::listDir(_zkCounterPath, fileList);
    if (ret != EC_OK) {
        BS_LOG(ERROR, "collect counter from zk[%s] fail!", _zkCounterPath.c_str());
        return false;
    }

    for (size_t i = 0; i < fileList.size(); i++) {
        string filePath = fslib::util::FileUtil::joinFilePath(_zkCounterPath, fileList[i]);
        bool fileExist = true;
        auto counterMap = CounterFileSynchronizer::loadCounterMap(filePath, fileExist);
        if (!counterMap || !fileExist) {
            continue;
        }
        fillRoleCounterMap(fileList[i], counterMap, roleCounterMap);
    }
    return true;
}

}} // namespace build_service::admin
