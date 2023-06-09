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
#include "ha3/sql/ops/tvf/builtin/ScoreModelTvfCreatorFactory.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorFactory.h"
#include "ha3/sql/ops/tvf/builtin/ScoreModelTvfFunc.h"

using namespace std;
namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, ScoreModelTvfCreatorFactory);

ScoreModelTvfCreatorFactory::ScoreModelTvfCreatorFactory() {}

ScoreModelTvfCreatorFactory::~ScoreModelTvfCreatorFactory() {}

bool ScoreModelTvfCreatorFactory::registerTvfFunctions() {
    REGISTER_TVF_FUNC_CREATOR("scoreModelTvf", ScoreModelTvfFuncCreator);
    return true;
}

} // namespace sql
} // namespace isearch
