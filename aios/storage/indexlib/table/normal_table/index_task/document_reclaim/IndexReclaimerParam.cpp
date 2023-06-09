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
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexReclaimerParam.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, IndexReclaimerParam);

const std::string IndexReclaimerParam::AND_RECLAIM_OPERATOR = "AND";
void IndexReclaimerParam::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("reclaim_index", _reclaimIndex, _reclaimIndex);
    json.Jsonize("reclaim_terms", _reclaimTerms, _reclaimTerms);
    json.Jsonize("reclaim_operator", _reclaimOperator, _reclaimOperator);
    json.Jsonize("reclaim_index_info", _reclaimOprands, _reclaimOprands);
}
} // namespace indexlibv2::table
