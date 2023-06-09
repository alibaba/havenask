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
#include <string>

#include "indexlib/table/index_task/merger/MergePlan.h"

namespace indexlibv2::framework {
class IndexTaskContext;
}

namespace indexlibv2 { namespace table {
/*
MergeStrategy 负责根据merge策略，从tabletData中挑选segment参与merge
MergeStrategy可以生成多组SegmentMergePlan, 每一个SegmentMergePlan中填充的是一起merge的segments
*/
class MergeStrategy
{
public:
    MergeStrategy() {}
    virtual ~MergeStrategy() {}

public:
    virtual std::string GetName() const = 0;
    virtual std::pair<Status, std::shared_ptr<MergePlan>>
    CreateMergePlan(const framework::IndexTaskContext* context) = 0;

protected:
    // 根据 Context 在 MergePlan 中补充 TargetSegment 和 TargetVersion 信息
    static Status FillMergePlanTargetInfo(const framework::IndexTaskContext* context,
                                          std::shared_ptr<MergePlan> mergePlan);

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
