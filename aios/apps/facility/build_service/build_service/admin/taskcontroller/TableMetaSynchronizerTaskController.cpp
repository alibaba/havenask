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
#include "build_service/admin/taskcontroller/TableMetaSynchronizerTaskController.h"

#include <iosfwd>

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TableMetaSyncronizerTaskController);

TableMetaSyncronizerTaskController::~TableMetaSyncronizerTaskController() {}

bool TableMetaSyncronizerTaskController::init(const std::string& clusterName, const std::string& taskConfigPath,
                                              const std::string& initParam)
{
    if (!DefaultTaskController::init(clusterName, taskConfigPath, initParam)) {
        return false;
    }
    if (_partitionCount * _parallelNum > 1) {
        BS_LOG(ERROR,
               "table meta syncronizer only support one instance,"
               "current partition count [%d], parallel num[%d]",
               _partitionCount, _parallelNum);
        return false;
    }
    return true;
}
}} // namespace build_service::admin
