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

#include "autil/Log.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/config/truncate_profile.h"
#include "indexlib/config/truncate_strategy.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class TruncateIndexProperty
{
public:
    TruncateIndexProperty();
    ~TruncateIndexProperty();

public:
    bool HasFilter() const { return mTruncateStrategy->GetDiversityConstrain().NeedFilter(); }

    bool IsFilterByMeta() const { return mTruncateStrategy->GetDiversityConstrain().IsFilterByMeta(); }

    bool IsFilterByTimeStamp() const { return mTruncateStrategy->GetDiversityConstrain().IsFilterByTimeStamp(); }

    bool HasDistinct() const { return mTruncateStrategy->GetDiversityConstrain().NeedDistinct(); }
    bool HasSort() const { return mTruncateProfile->HasSort(); }

    size_t GetSortDimenNum() const { return mTruncateProfile->GetSortDimenNum(); }

    std::string GetStrategyType() const { return mTruncateStrategy->GetStrategyType(); }

    uint64_t GetThreshold() const { return mTruncateStrategy->GetThreshold(); }

public:
    std::string mTruncateIndexName;
    TruncateStrategyPtr mTruncateStrategy;
    TruncateProfilePtr mTruncateProfile;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::vector<TruncateIndexProperty> TruncateIndexPropertyVector;

typedef std::shared_ptr<TruncateIndexProperty> TruncateIndexPropertyPtr;
}} // namespace indexlib::config
