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
#include "ha3/sql/ops/planTransform/TableScanOpUtil.h"

#include <unordered_map>

#include "iquan/common/Common.h"

using namespace iquan;

namespace isearch {
namespace sql {

static std::unordered_map<std::string, std::string> tableType2KernelMap = {
    {IQUAN_TABLE_TYPE_EXTERNAL, ASYNC_SCAN_OP},
    {IQUAN_TABLE_TYPE_KV, ASYNC_SCAN_OP},
    {IQUAN_TABLE_TYPE_SUMMARY, ASYNC_SCAN_OP},
    {IQUAN_TABLE_TYPE_KHRONOS_META, KHRONOS_SCAN_OP},
    {IQUAN_TABLE_TYPE_KHRONOS_DATA, KHRONOS_SCAN_OP},
    {IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA, KHRONOS_SCAN_OP},
    {IQUAN_TABLE_TYPE_ORC, TABLET_ORC_SCAN_OP},
    {IQUAN_TABLE_TYPE_AGGREGATION, ASYNC_SCAN_OP}
};

const std::string &TableScanOpUtil::getScanKernelName(const std::string &tableType) {
    static std::string defaultKernel = SCAN_OP;
    auto it = tableType2KernelMap.find(tableType);
    if (it != tableType2KernelMap.end()) {
        return it->second;
    }
    return defaultKernel;
}

} // namespace sql
} // namespace isearch
