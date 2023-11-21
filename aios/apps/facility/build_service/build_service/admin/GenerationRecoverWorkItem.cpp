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
#include "build_service/admin/GenerationRecoverWorkItem.h"

#include <memory>

#include "alog/Logger.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common/PathDefine.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::common;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, GenerationRecoverWorkItem);

GenerationRecoverWorkItem::GenerationRecoverWorkItem(const string& zkRoot, const BuildId& buildId,
                                                     const GenerationKeeperPtr& keeper)
    : _zkRoot(zkRoot)
    , _buildId(buildId)
    , _keeper(keeper)
    , _status(RECOVER_FAILED)
{
}

GenerationRecoverWorkItem::~GenerationRecoverWorkItem() {}

void GenerationRecoverWorkItem::process()
{
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, _buildId);
    string generationStopFile = PathDefine::getGenerationStopFile(_zkRoot, _buildId);
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(generationStatusFile, exist)) {
        BS_LOG(ERROR, "check whether %s exist failed.", generationStatusFile.c_str());
        return;
    }
    if (exist) {
        if (!recoverOneGenerationKeeper(_buildId, false)) {
            return;
        }
        _status = RECOVER_SUCCESS;
        return;
    }
    if (!fslib::util::FileUtil::isExist(generationStopFile, exist)) {
        BS_LOG(ERROR, "check whether %s exist failed.", generationStopFile.c_str());
        return;
    }
    if (exist) {
        if (!recoverOneGenerationKeeper(_buildId, true)) {
            return;
        }
        _status = RECOVER_SUCCESS;
    } else {
        _status = NO_NEED_RECOVER;
    }
}

bool GenerationRecoverWorkItem::recoverOneGenerationKeeper(const BuildId& buildId, bool isStopped)
{
    BS_LOG(INFO, "recover generation [%s] from [%s] .", buildId.ShortDebugString().c_str(), _zkRoot.c_str());
    if (!isStopped) {
        if (!_keeper->recover()) {
            string errorMsg =
                "recover active generation [" + buildId.ShortDebugString() + "] from [" + _zkRoot + "] failed.";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    } else {
        if (!_keeper->recoverStoppedKeeper()) {
            string errorMsg =
                "recover stopped generation [" + buildId.ShortDebugString() + "] from [" + _zkRoot + "] failed.";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    BS_LOG(INFO, "recover generation [%s] success.", buildId.ShortDebugString().c_str());
    return true;
}

}} // namespace build_service::admin
