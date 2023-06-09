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

#include "autil/Log.h"
#include "table/Table.h"
#include "tensorflow/core/framework/types.pb.h"

namespace tensorflow {
class Tensor;
} // namespace tensorflow

namespace isearch {
namespace sql {

class TableToTensor {
public:
    static bool convertColumnToTensor(const table::TablePtr &table,
                                      const std::string &columnName,
                                      const tensorflow::DataType &type,
                                      tensorflow::Tensor &tensor);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
