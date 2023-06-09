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
#include "ha3/sql/ops/scan/builtin/AithetaUdfToQueryCreatorFactory.h"

#include "alog/Logger.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/scan/builtin/AithetaUdfToQuery.h"

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, AithetaUdfToQueryCreatorFactory);

bool AithetaUdfToQueryCreatorFactory::registerUdfToQuery(autil::InterfaceManager *manager) {
    REGISTER_INTERFACE(SQL_UDF_AITHETA_OP, AithetaUdfToQuery, manager);
    return true;
}

} // namespace sql
} // namespace isearch
