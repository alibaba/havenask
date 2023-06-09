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
#include "build_service/custom_merger/BuildinCustomMergerFactory.h"

#include "build_service/custom_merger/AlterDefaultFieldMerger.h"
#include "build_service/custom_merger/ReuseAttributeAlterFieldMerger.h"

using namespace std;

namespace build_service { namespace custom_merger {
BS_LOG_SETUP(custom_merger, BuildinCustomMergerFactory);

BuildinCustomMergerFactory::BuildinCustomMergerFactory() {}

BuildinCustomMergerFactory::~BuildinCustomMergerFactory() {}

CustomMerger* BuildinCustomMergerFactory::createCustomMerger(const std::string& mergerName, uint32_t backupId,
                                                             const string& epochId)
{
#define ENUM_MERGER(merger)                                                                                            \
    if (mergerName == merger::MERGER_NAME) {                                                                           \
        return new merger(backupId, epochId);                                                                          \
    }

    ENUM_MERGER(AlterDefaultFieldMerger);
    ENUM_MERGER(ReuseAttributeAlterFieldMerger);
    return NULL;
}

}} // namespace build_service::custom_merger
