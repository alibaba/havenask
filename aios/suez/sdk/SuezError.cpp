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
#include "suez/sdk/SuezError.h"

namespace suez {

const SuezError ERROR_NONE;
const SuezError DEPLOY_CONFIG_ERROR(1, "deploy error: deploy config failed");
const SuezError DEPLOY_INDEX_ERROR(2, "deploy error: deploy index failed");
const SuezError DEPLOY_DATA_ERROR(3, "deploy error: deploy data failed");
const SuezError CONFIG_LOAD_ERROR(4, "load error: invalid table config");
const SuezError LOAD_UNKNOWN_ERROR(5, "load error: unknown");
const SuezError LOAD_LACKMEM_ERROR(6, "load error: lack of memory");
const SuezError LOAD_FORCE_REOPEN_UNKNOWN_ERROR(7, "force reopen error: unknown");
const SuezError LOAD_FORCE_REOPEN_LACKMEM_ERROR(8, "force reopen error: lack of memory");
const SuezError LOAD_RT_ERROR(9, "loadRt error: start RealtimeBuilder failed");
const SuezError CREATE_TABLE_ERROR(10, "create table error: get table type failed");
const SuezError LOAD_RAW_FILE_TABLE_ERROR(11, "load raw file table error: load schema failed");
const SuezError DEPLOY_DISK_QUOTA_ERROR(12, "deploy error: disk quota exhausted");
const SuezError UPDATE_RT_ERROR(13, "update rt error: no rtbuilder");
const SuezError FORCE_RELOAD_ERROR(14, "load error: force reload");

} // namespace suez
