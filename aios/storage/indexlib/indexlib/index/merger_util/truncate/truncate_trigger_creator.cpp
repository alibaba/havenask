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
#include "indexlib/index/merger_util/truncate/truncate_trigger_creator.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/truncate_index_property.h"
#include "indexlib/index/merger_util/truncate/default_truncate_trigger.h"
#include "indexlib/index/merger_util/truncate/truncate_meta_trigger.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, TruncateTriggerCreator);

TruncateTriggerCreator::TruncateTriggerCreator() {}

TruncateTriggerCreator::~TruncateTriggerCreator() {}

TruncateTriggerPtr TruncateTriggerCreator::Create(const TruncateIndexProperty& truncateIndexProperty)
{
    const string& strategyType = truncateIndexProperty.GetStrategyType();
    uint64_t threshold = truncateIndexProperty.GetThreshold();
    TruncateTriggerPtr truncStrategy;
    if (strategyType == TRUNCATE_META_STRATEGY_TYPE) {
        truncStrategy.reset(new TruncateMetaTrigger(threshold));
    } else {
        truncStrategy.reset(new DefaultTruncateTrigger(threshold));
    }
    return truncStrategy;
}
} // namespace indexlib::index::legacy
