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

#include <memory>
#include <string>
#include <vector>

namespace table {
class Table;
};

namespace autil {
namespace mem_pool {
class Pool;
}
}; // namespace autil

namespace indexlibv2 {
namespace index {
class RowGroup;
}
}

namespace isearch {
namespace sql {

class RowGroupToTableConverter {
public:
    static bool convert(indexlibv2::index::RowGroup *rowGroup,
                        const std::vector<std::string> &requiredCols,
                        table::Table *table);
};

} // namespace sql
} // namespace isearch
