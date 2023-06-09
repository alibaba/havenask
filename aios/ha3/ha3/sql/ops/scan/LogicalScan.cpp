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
#include "ha3/sql/ops/scan/LogicalScan.h"

#include <stdint.h>
#include <cstddef>
#include <memory>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "ha3/common/Term.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/ScanUtil.h"
#include "ha3/sql/ops/scan/ScanResource.h"
#include "matchdoc/ValueType.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "table/Row.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace suez::turing;

using namespace isearch::common;
namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, LogicalScan);

LogicalScan::LogicalScan(const ScanInitParam &param)
    : ScanBase(param) {
    _enableTableInfo = false;
}

LogicalScan::~LogicalScan() {}

bool LogicalScan::doInit() {
    return true;
}

bool LogicalScan::doBatchScan(TablePtr &table, bool &eof) {
    eof = true;

    autil::ScopedTime2 outputTimer;
    table = createTable();
    incOutputTime(outputTimer.done_us());

    return true;
}

TablePtr LogicalScan::createTable() {
    return ScanUtil::createEmptyTable(_param.outputFields, _param.outputFieldsType, _param.scanResource.memoryPoolResource->getPool());
}

} // namespace sql
} // namespace isearch
