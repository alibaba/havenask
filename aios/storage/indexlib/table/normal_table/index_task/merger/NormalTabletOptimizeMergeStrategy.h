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

#include "autil/Log.h"
#include "indexlib/table/index_task/merger/OptimizeMergeStrategy.h"

namespace indexlibv2 { namespace table {

class NormalTabletOptimizeMergeStrategy : public OptimizeMergeStrategy
{
public:
    NormalTabletOptimizeMergeStrategy();
    ~NormalTabletOptimizeMergeStrategy();

private:
    std::pair<Status, int64_t>
    GetSegmentValidDocCount(const std::shared_ptr<framework::Segment>& segment) const override;

private:
    friend class NormalTabletOptimizeMergeStrategyTest;
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
