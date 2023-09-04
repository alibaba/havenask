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

#include <string>

#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/operation_log/Common.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/primary_key/Common.h"

namespace indexlibv2::table {
static constexpr char OPERATION_LOG_TO_PATCH_WORK_DIR[] = "operation_log_to_patch_work_dir";
static constexpr char OPERATION_LOG_TO_PATCH_META_FILE_NAME[] = "operation_log_to_patch.meta";

} // namespace indexlibv2::table
