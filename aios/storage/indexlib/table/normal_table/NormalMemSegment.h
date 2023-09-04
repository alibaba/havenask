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
#include "autil/NoCopyable.h"
#include "indexlib/table/plain/PlainMemSegment.h"

namespace indexlib::util {
class BuildResourceMetricsNode;
}

namespace indexlibv2::table {

class NormalMemSegment : public plain::PlainMemSegment
{
public:
    NormalMemSegment(const config::TabletOptions* options, const std::shared_ptr<config::ITabletSchema>& schema,
                     const framework::SegmentMeta& segmentMeta);
    ~NormalMemSegment();

protected:
    std::pair<Status, std::shared_ptr<framework::DumpParams>> CreateDumpParams() override;
    void CalcMemCostInCreateDumpParams() override;

private:
    indexlib::util::BuildResourceMetricsNode* _buildResourceMetricsNode;
    int64_t _dumpMemCostPerDoc;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
