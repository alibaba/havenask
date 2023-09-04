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
#include "suez/common/GeneratorDef.h"

using namespace std;
namespace suez {

const std::string GeneratorDef::LEADER_TAG = "leader";

static const string BIZ_NAME_SEP = ".";
static const string BIZ_NAME_SUFFIX = ".write";

std::string GeneratorDef::getTableWriteBizName(const string &zoneName, const string &tableName, bool needZoneName) {
    return (needZoneName ? zoneName + BIZ_NAME_SEP + tableName + BIZ_NAME_SUFFIX : tableName + BIZ_NAME_SUFFIX);
}

} // namespace suez
