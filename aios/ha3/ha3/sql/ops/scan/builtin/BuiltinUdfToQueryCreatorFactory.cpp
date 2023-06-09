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
#include "ha3/sql/ops/scan/builtin/BuiltinUdfToQueryCreatorFactory.h"

#include "alog/Logger.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/scan/builtin/ContainUdfToQuery.h"
#include "ha3/sql/ops/scan/builtin/MatchIndexUdfToQuery.h"
#include "ha3/sql/ops/scan/builtin/PidvidContainUdfToQuery.h"
#include "ha3/sql/ops/scan/builtin/QueryUdfToQuery.h"
#include "ha3/sql/ops/scan/builtin/SpUdfToQuery.h"
#include "ha3/sql/ops/scan/builtin/InUdfToQuery.h"
#include "ha3/sql/ops/scan/builtin/KvExtractMatchUdfToQuery.h"

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, BuiltinUdfToQueryCreatorFactory);

bool BuiltinUdfToQueryCreatorFactory::registerUdfToQuery(autil::InterfaceManager *manager) {
    REGISTER_INTERFACE(SQL_UDF_MATCH_OP, MatchIndexUdfToQuery, manager);
    REGISTER_INTERFACE(SQL_UDF_QUERY_OP, QueryUdfToQuery, manager);
    REGISTER_INTERFACE(SQL_UDF_CONTAIN_OP, ContainUdfToQuery, manager);
    REGISTER_INTERFACE(SQL_UDF_HA_IN_OP, ContainUdfToQuery, manager);
    REGISTER_INTERFACE(SQL_UDF_PIDVID_CONTAIN_OP, PidvidContainUdfToQuery, manager);
    REGISTER_INTERFACE(SQL_UDF_SP_QUERY_MATCH_OP, SpUdfToQuery, manager);
    REGISTER_INTERFACE(SQL_IN_OP, InUdfToQuery, manager);
    REGISTER_INTERFACE(SQL_UDF_KV_EXTRACT_MATCH_OP, KvExtractMatchUdfToQuery, manager);
    return true;
}

} // namespace sql
} // namespace isearch
