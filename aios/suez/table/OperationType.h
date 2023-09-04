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

namespace suez {

enum OperationType {
    OP_INIT = 0,
    OP_DEPLOY = 1,
    OP_CANCELDEPLOY = 2,
    OP_UPDATERT = 3,
    OP_LOAD = 4,
    OP_UNLOAD = 5,
    OP_CANCEL = 6,
    OP_REMOVE = 7,
    OP_FORCELOAD = 8,
    OP_CANCELLOAD = 9,
    OP_RELOAD = 10,

    // for table pre-loading according to final target.
    OP_PRELOAD = 11,

    // 当load config path或者index root变化时进入此状态，需要重新check & deploy 所有文件
    OP_DIST_DEPLOY = 16,
    OP_HOLD = 17,
    OP_CLEAN_DISK = 18,
    OP_FINAL_TO_TARGET = 19,
    OP_CLEAN_INC_VERSION = 20,

    // commit
    OP_COMMIT = 50,
    OP_SYNC_VERSION = 51,

    OP_BECOME_LEADER = 60,
    OP_NO_LONGER_LEADER = 61,

    // trivial operation
    OP_UPDATEKEEPCOUNT = 100,
    OP_UPDATE_CONFIG_KEEPCOUNT = 101,
    OP_NONE = 200,
    OP_INVALID = 201
};

extern const char *opName(OperationType op);
extern const char *OperationType_Name(OperationType op);
extern bool isDeployOperation(OperationType op);

} // namespace suez
