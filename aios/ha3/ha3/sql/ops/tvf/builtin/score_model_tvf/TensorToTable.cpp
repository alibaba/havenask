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
#include "ha3/sql/ops/tvf/builtin/TensorToTable.h"

#include <memory>
#include <stddef.h>

#include "alog/Logger.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "table/ColumnData.h"
#include "table/Row.h"
#include "table/TableUtil.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "turing_ops_util/util/OpUtil.h"

using namespace tensorflow;
using namespace table;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, TensorToTable);

bool TensorToTable::convertTensorToColumn(const tensorflow::Tensor &tensor,
                                          const std::string &columnName,
                                          autil::mem_pool::Pool *pool,
                                          TablePtr &table) {
    size_t rowCount = table->getRowCount();
    int dims = tensor.dims();
    int embNums = 1;
    for (int idx = 0; idx < dims; ++idx) {
        embNums *= tensor.dim_size(idx);
    }
    if (rowCount != embNums) {
        SQL_LOG(ERROR,
                "column [%s] row count[%lu] is not equal to tensor size [%d]",
                columnName.c_str(),
                rowCount,
                embNums);
        return false;
    }
#define CASE_MACRO(label)                                                                          \
    case label: {                                                                                  \
        typedef EnumToDataType<label>::Type TFType;                                                \
        typedef typename suez::turing::TFType2Type<TFType>::Type T;                                \
        ColumnData<T> *column                                                                      \
            = TableUtil::declareAndGetColumnData<T>(table, columnName, true, true);                \
        if (!column) {                                                                             \
            SQL_LOG(ERROR, "declare and get column data[%s] failed", columnName.c_str());          \
            return false;                                                                          \
        }                                                                                          \
        auto *pdata = tensor.base<T>();                                                            \
        for (size_t i = 0; i < rowCount; i++) {                                                    \
            column->set(i, pdata[i]);                                                              \
        }                                                                                          \
        break;                                                                                     \
    }

    switch (tensor.dtype()) {
    case DT_STRING: {
        ColumnData<autil::MultiChar> *column
            = TableUtil::declareAndGetColumnData<autil::MultiChar>(table, columnName, true, true);
        if (!column) {
            SQL_LOG(ERROR, "declare and get column data[%s] failed", columnName.c_str());
            return false;
        }
        auto *pdata = tensor.base<string>();
        for (size_t i = 0; i < rowCount; i++) {
            char *buf = autil::MultiValueCreator::createMultiValueBuffer(
                pdata[i].data(), pdata[i].size(), pool);
            column->set(i, autil::MultiChar(buf));
        }
        break;
    }
        CASE_MACRO(DT_INT32);
        CASE_MACRO(DT_INT64);
        CASE_MACRO(DT_DOUBLE);
        CASE_MACRO(DT_FLOAT);
    default: {
        SQL_LOG(ERROR, "not support date type [%s]", DataTypeString(tensor.dtype()).c_str());
        return false;
    }
    }
    return true;
}

} // namespace sql
} // namespace isearch
