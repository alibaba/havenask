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
#include "indexlib/index/inverted_index/truncate/TruncateTriggerCreator.h"

#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/index/inverted_index/truncate/DefaultTruncateTrigger.h"
#include "indexlib/index/inverted_index/truncate/TruncateMetaTrigger.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, TruncateTriggerCreator);

std::shared_ptr<TruncateTrigger>
TruncateTriggerCreator::Create(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty)
{
    const std::string& strategyType = truncateIndexProperty.GetStrategyType();
    uint64_t threshold = truncateIndexProperty.GetThreshold();
    std::shared_ptr<TruncateTrigger> truncStrategy;
    if (strategyType == TRUNCATE_META_STRATEGY_TYPE) {
        truncStrategy.reset(new TruncateMetaTrigger());
    } else {
        truncStrategy.reset(new DefaultTruncateTrigger(threshold));
    }
    return truncStrategy;
}

} // namespace indexlib::index
