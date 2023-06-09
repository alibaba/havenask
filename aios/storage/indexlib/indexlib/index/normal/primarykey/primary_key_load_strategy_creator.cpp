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
#include "indexlib/index/normal/primarykey/primary_key_load_strategy_creator.h"

#include "indexlib/index/normal/primarykey/combine_segments_primary_key_load_strategy.h"
#include "indexlib/index/normal/primarykey/default_primary_key_load_strategy.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PrimaryKeyLoadStrategyCreator);

PrimaryKeyLoadStrategyCreator::PrimaryKeyLoadStrategyCreator() {}

PrimaryKeyLoadStrategyCreator::~PrimaryKeyLoadStrategyCreator() {}

PrimaryKeyLoadStrategyPtr
PrimaryKeyLoadStrategyCreator::CreatePrimaryKeyLoadStrategy(const config::PrimaryKeyIndexConfigPtr& pkIndexConfig)
{
    PrimaryKeyLoadStrategyParamPtr param = pkIndexConfig->GetPKLoadStrategyParam();
    assert(param);
    if (param->NeedCombineSegments()) {
        return PrimaryKeyLoadStrategyPtr(new CombineSegmentsPrimaryKeyLoadStrategy(pkIndexConfig));
    }
    return PrimaryKeyLoadStrategyPtr(new DefaultPrimaryKeyLoadStrategy(pkIndexConfig));
}
}} // namespace indexlib::index
