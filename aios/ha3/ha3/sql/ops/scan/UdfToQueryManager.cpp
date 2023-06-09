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
#include "ha3/sql/ops/scan/UdfToQueryManager.h"

#include <memory>
#include <utility>

#include "ha3/sql/ops/scan/builtin/BuiltinUdfToQueryCreatorFactory.h"

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, UdfToQueryManager);

#define REGISTER_UDF_TO_QUERY_FACTORY(factory)                  \
    do {                                                        \
        if (!factory.registerUdfToQuery(this)) {                \
            return false;                                       \
        }                                                       \
    } while (false)

bool UdfToQueryManager::init() {
    BuiltinUdfToQueryCreatorFactory factory;
    REGISTER_UDF_TO_QUERY_FACTORY(factory);
    return true;
}

#undef REGISTER_UDF_TO_QUERY_FACTORY

} // namespace sql
} // namespace isearch
