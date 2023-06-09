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
#pragma once

#include "suez/sdk/Magic.h"

namespace suez {

enum SuezPartitionType
{
    SPT_INDEXLIB = 0,
    SPT_RAWFILE = 1,
    SPT_TABLET = 2,
    SPT_NONE = 3,
};

enum UPDATE_RESULT
{
    UR_REACH_TARGET = 1,
    UR_NEED_RETRY = 2,
    UR_ERROR = 3,
};

ENUM_TO_STR(UPDATE_RESULT, UR_REACH_TARGET, UR_NEED_RETRY, UR_ERROR)

} // namespace suez
