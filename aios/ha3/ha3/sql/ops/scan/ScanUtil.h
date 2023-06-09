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
#include <vector>

namespace autil {
class HashFunctionBase;
} // namespace autil

namespace suez {
namespace turing {
class IndexInfo;
} // namespace turing
} // namespace suez

namespace table {
class Table;
}

namespace autil {
namespace mem_pool {
class Pool;
}
}

namespace isearch {
namespace sql {

struct ScanInitParam;

class ScanUtil {
public:
    ScanUtil() = delete;
    ~ScanUtil() = delete;
    ScanUtil(const ScanUtil &) = delete;
    ScanUtil &operator=(const ScanUtil &) = delete;
public:
    static void filterPksByParam(const ScanInitParam &param,
                                 const suez::turing::IndexInfo *pkIndexInfo,
                                 std::vector<std::string> &pks);
    static std::string indexTypeToString(uint32_t indexType);
    static std::shared_ptr<table::Table> createEmptyTable(const std::vector<std::string> &outputFileds, const std::vector<std::string> &outputFiledsType, const std::shared_ptr<autil::mem_pool::Pool> &pool);

private:
    static std::shared_ptr<autil::HashFunctionBase> createHashFunc(const ScanInitParam &param);
    static void filterPksByParallel(const ScanInitParam &param,
                                    std::vector<std::string> &pks);
    static void filterPksByHashRange(const ScanInitParam &param,
                                     const suez::turing::IndexInfo *pkIndexInfo,
                                     std::vector<std::string> &pks);
};

} // namespace sql
} // namespace isearch
