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
#include "ha3/sql/ops/values/ValuesKernel.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <stdint.h>

#include "alog/Logger.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace navi {
class KernelInitContext;
} // namespace navi

using namespace std;
using namespace table;
using namespace matchdoc;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, ValuesKernel);
ValuesKernel::ValuesKernel()
    : _memoryPoolResource(nullptr) {}

ValuesKernel::~ValuesKernel() {}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
void ValuesKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("ValuesKernel")
        .output("output0", TableType::TYPE_ID)
        .resource(navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource));
}

bool ValuesKernel::config(navi::KernelConfigContext &ctx) {
    ctx.Jsonize("output_fields", _outputFields);
    ctx.Jsonize("output_fields_type", _outputTypes);
    if (_outputFields.size() != _outputTypes.size()) {
        SQL_LOG(ERROR,
                "output fields size[%lu] not equal output types size[%lu]",
                _outputFields.size(),
                _outputTypes.size());
        return false;
    }
    KernelUtil::stripName(_outputFields);
    return true;
}

navi::ErrorCode ValuesKernel::init(navi::KernelInitContext &context) {
    return navi::EC_NONE;
}

navi::ErrorCode ValuesKernel::compute(navi::KernelComputeContext &runContext) {
#define CHECK_DECLARE_COLUMN(columnData)                                                           \
    if (columnData == nullptr) {                                                                   \
        SQL_LOG(ERROR,                                                                             \
                "declare column [%s] failed, type [%s]",                                           \
                _outputFields[i].c_str(),                                                          \
                _outputTypes[i].c_str());                                                          \
        return navi::EC_ABORT;                                                                     \
    }

    TablePtr table(new Table(_memoryPoolResource->getPool()));
    for (size_t i = 0; i < _outputFields.size(); ++i) {
        if (_outputTypes[i] == "BOOLEAN") {
            auto columnData
                = TableUtil::declareAndGetColumnData<bool>(table, _outputFields[i], false);
            CHECK_DECLARE_COLUMN(columnData);
        } else if (_outputTypes[i] == "TINYINT" || _outputTypes[i] == "SMALLINT"
                   || _outputTypes[i] == "INTEGER" || _outputTypes[i] == "BIGINT") {
            auto columnData
                = TableUtil::declareAndGetColumnData<int64_t>(table, _outputFields[i], false);
            CHECK_DECLARE_COLUMN(columnData);
        } else if (_outputTypes[i] == "DECIMAL" || _outputTypes[i] == "FLOAT"
                   || _outputTypes[i] == "REAL" || _outputTypes[i] == "DOUBLE") {
            auto columnData
                = TableUtil::declareAndGetColumnData<double>(table, _outputFields[i], false);
            CHECK_DECLARE_COLUMN(columnData);
        } else if (_outputTypes[i] == "VARCHAR") {
            auto columnData = TableUtil::declareAndGetColumnData<autil::MultiChar>(
                table, _outputFields[i], false);
            CHECK_DECLARE_COLUMN(columnData);
        } else {
            SQL_LOG(ERROR,
                    "output field [%s] with type [%s] not supported",
                    _outputFields[i].c_str(),
                    _outputTypes[i].c_str());
            return navi::EC_ABORT;
        }
    }
    table->endGroup();
    navi::PortIndex index(0, navi::INVALID_INDEX);
    TableDataPtr tableData(new TableData(table));
    runContext.setOutput(index, tableData, true);
    return navi::EC_NONE;
}

REGISTER_KERNEL(ValuesKernel);

} // namespace sql
} // namespace isearch
