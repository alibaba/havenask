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
#include "ha3/sql/ops/tvf/builtin/BuiltinTvfFuncCreatorFactory.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorFactory.h"
#include "ha3/sql/ops/tvf/builtin/DistinctTopNTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/EnableShuffleTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/IdentityTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/InputTableTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/PrintTableTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/RankTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/RerankByQuotaTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/SimpleRerankTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/SortTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/TableMetricTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/TaobaoSpRerankTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/UnPackMultiValueTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/UnorderRankTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/TopKTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/TopKMarkTvfFunc.h"

using namespace std;
namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, BuiltinTvfFuncCreatorFactory);

BuiltinTvfFuncCreatorFactory::BuiltinTvfFuncCreatorFactory() {}

BuiltinTvfFuncCreatorFactory::~BuiltinTvfFuncCreatorFactory() {}

bool BuiltinTvfFuncCreatorFactory::registerTvfFunctions() {
    REGISTER_TVF_FUNC_CREATOR("unpackMultiValue", UnPackMultiValueTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("printTableTvf", PrintTableTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("rankTvf", RankTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("sortTvf", SortTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("enableShuffleTvf", EnableShuffleTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("inputTableTvf", InputTableTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("inputTableWithSepTvf", inputTableWithSepTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("distinctTopNTvf", DistinctTopNTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("rerankByQuotaTvf", RerankByQuotaTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("identityTvf", IdentityTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("simpleRerankTvf", SimpleRerankTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("unorderRankTvf", UnorderRankTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("taobaoSpRerankTvf", TaobaoSpRerankTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("tableMetricTvf", TableMetricTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("topKTvf", TopKTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("topKMarkTvf", TopKMarkTvfFuncCreator);
    return true;
}

} // namespace sql
} // namespace isearch
