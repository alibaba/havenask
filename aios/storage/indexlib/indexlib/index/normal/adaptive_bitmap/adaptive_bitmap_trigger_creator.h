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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

namespace indexlib { namespace index { namespace legacy {
class AdaptiveBitmapTrigger;

class AdaptiveBitmapTriggerCreator
{
public:
    AdaptiveBitmapTriggerCreator(const ReclaimMapPtr& reclaimMap);
    ~AdaptiveBitmapTriggerCreator();

public:
    std::shared_ptr<AdaptiveBitmapTrigger> Create(const config::IndexConfigPtr& indexConf);

private:
    ReclaimMapPtr mReclaimMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveBitmapTriggerCreator);
}}} // namespace indexlib::index::legacy
