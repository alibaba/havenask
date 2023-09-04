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
#include "sql/ops/scan/udf_to_query/BuiltinUdfToQueryCreatorFactory.h"

#include "autil/plugin/InterfaceManager.h"
#include "sql/common/common.h"
#include "sql/ops/scan/udf_to_query/ContainUdfToQuery.h"
#include "sql/ops/scan/udf_to_query/InUdfToQuery.h"
#include "sql/ops/scan/udf_to_query/KvExtractMatchUdfToQuery.h"
#include "sql/ops/scan/udf_to_query/MatchIndexUdfToQuery.h"
#include "sql/ops/scan/udf_to_query/PidvidContainUdfToQuery.h"
#include "sql/ops/scan/udf_to_query/QueryUdfToQuery.h"
#include "sql/ops/scan/udf_to_query/SpUdfToQuery.h"

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
