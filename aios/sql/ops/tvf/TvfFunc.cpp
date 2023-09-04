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
#include "sql/ops/tvf/TvfFunc.h"

#include <iosfwd>
#include <memory>

#include "sql/common/Log.h"
#include "table/Row.h"
#include "table/Table.h"

using namespace std;
using namespace table;
namespace sql {

bool OnePassTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    if (_table == nullptr) {
        _table = input;
    } else {
        if (input != nullptr && !_table->merge(input)) {
            SQL_LOG(ERROR, "tvf [%s] merge input table failed", getName().c_str());
            return false;
        }
    }
    if (eof) {
        if (_table == nullptr) {
            SQL_LOG(ERROR, "tvf [%s] input is null", getName().c_str());
            return false;
        }
        bool ret = doCompute(_table, output);
        if (output == nullptr) {
            SQL_LOG(ERROR, "tvf [%s] output is null", getName().c_str());
            return false;
        }
        _table.reset();
        return ret;
    }
    return true;
}

} // namespace sql
