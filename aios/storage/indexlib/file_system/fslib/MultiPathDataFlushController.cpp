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
#include "indexlib/file_system/fslib/MultiPathDataFlushController.h"

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <stdlib.h>

#include "autil/StringUtil.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MultiPathDataFlushController);

MultiPathDataFlushController::MultiPathDataFlushController() { InitFromEnv(); }

MultiPathDataFlushController::~MultiPathDataFlushController() {}

void MultiPathDataFlushController::InitFromEnv()
{
    char* envParam = getenv("INDEXLIB_DATA_FLUSH_PARAM");
    string paramStr;
    if (envParam) {
        paramStr = string(envParam);
    }

    InitFromString(paramStr);
}

void MultiPathDataFlushController::Clear() { _dataFlushControllers.clear(); }

void MultiPathDataFlushController::InitFromString(const std::string& initStr)
{
    vector<string> patternStrVec;
    autil::StringUtil::fromString(initStr, patternStrVec, ";");
    for (auto& pattern : patternStrVec) {
        DataFlushControllerPtr controller(new DataFlushController());
        controller->InitFromString(pattern);
        _dataFlushControllers.push_back(controller);
    }
    // push a controller which covers all path
    DataFlushControllerPtr controller(new DataFlushController());
    controller->InitFromString("");
    _dataFlushControllers.push_back(controller);
}

DataFlushController* MultiPathDataFlushController::GetDataFlushController(const std::string& filePath) noexcept
{
    MultiPathDataFlushController* multiFlushController = MultiPathDataFlushController::GetInstance();
    return multiFlushController->FindDataFlushController(filePath);
}

DataFlushController* MultiPathDataFlushController::FindDataFlushController(const std::string& filePath) const noexcept
{
    for (auto controller : _dataFlushControllers) {
        if (controller->MatchPathPrefix(filePath)) {
            return controller.get();
        }
    }
    assert(false);
    return NULL;
}
}} // namespace indexlib::file_system
