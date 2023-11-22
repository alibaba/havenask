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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

#define ADMIN_STATUS_COLLECTOR_NAME "admin_status"
#define BATCHMODE_INFO_COLLECTOR_NAME "batch_status"
#define GENERATION_STATUS_COLLECTOR_NAME "generation_status"
#define GENERATION_ERROR_COLLECTOR_NAME "generation_error"
#define GENERATION_CMD_COLLECTOR_NAME "generation_command"
#define WORKER_STATUS_COLLECTOR_NAME "bs_worker_status"
#define WORKER_ERROR_COLLECTOR_NAME "bs_worker_error"
#define WORKER_PROCESS_ERROR_COLLECTOR_NAME "bs_worker_process_error"

}} // namespace build_service::common
