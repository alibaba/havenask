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

enum DeployStatus {
    DS_UNKNOWN = 0,
    DS_DEPLOYING = 1,
    DS_DEPLOYDONE = 2,
    DS_CANCELLED = 3,
    DS_DISKQUOTA = 4,
    DS_FAILED = 100,
};

ENUM_TO_STR(DeployStatus, DS_UNKNOWN, DS_DEPLOYING, DS_DEPLOYDONE, DS_CANCELLED, DS_DISKQUOTA, DS_FAILED)

enum TableRtStatus {
    TRS_BUILDING = 0,
    TRS_SUSPENDED = 1,
};

ENUM_TO_STR(TableRtStatus, TRS_BUILDING, TRS_SUSPENDED)

enum TableStatus {
    TS_UNKNOWN = 0,
    TS_INITIALIZING = 1,
    TS_UNLOADED = 2,
    TS_LOADING = 3,
    TS_UNLOADING = 4,
    // for gs
    TS_FORCELOADING = 5,
    TS_LOADED = 6,
    TS_FORCE_RELOAD = 7,

    // for table pre-loading according to final target.
    TS_PRELOADING = 51,
    TS_PRELOAD_FAILED = 52,
    TS_PRELOAD_FORCE_RELOAD = 53,
    // error states.
    TS_ERROR_LACK_MEM = 100,
    TS_ERROR_CONFIG = 101,
    TS_ERROR_UNKNOWN = 102,

    // role switching (leader -> follower, follower -> leader)
    TS_ROLE_SWITCHING = 150,
    TS_ROLE_SWITCH_ERROR = 151,

    // commit states
    TS_COMMITTING = 200,
    TS_COMMIT_ERROR = 201
};

ENUM_TO_STR(TableStatus,
            TS_UNKNOWN,
            TS_INITIALIZING,
            TS_UNLOADED,
            TS_LOADING,
            TS_UNLOADING,
            TS_FORCELOADING,
            TS_LOADED,
            TS_FORCE_RELOAD,
            TS_PRELOADING,
            TS_PRELOAD_FAILED,
            TS_PRELOAD_FORCE_RELOAD,
            TS_ERROR_LACK_MEM,
            TS_ERROR_CONFIG,
            TS_ERROR_UNKNOWN,
            TS_COMMITTING,
            TS_COMMIT_ERROR)

enum TableLoadType {
    TLT_TRY_LOAD_FIRST = 0,
    TLT_UNLOAD_FIRST = 1,
};

ENUM_TO_STR(TableLoadType, TLT_TRY_LOAD_FIRST, TLT_UNLOAD_FIRST)

enum RoleType {
    RT_LEADER,
    RT_FOLLOWER,
};
extern const char *RoleType_Name(RoleType type);

enum CampaginLeaderType {
    CLT_UNKNOWN,
    CLT_NORMAL,
    CLT_BY_INDICATION,
};

#define INVALID_TARGET_VERSION (-1)

} // namespace suez
