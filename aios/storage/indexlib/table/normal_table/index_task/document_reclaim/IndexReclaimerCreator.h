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
#include "indexlib/base/Types.h"

namespace kmonitor {
class MetricsReporter;
}
namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::framework {
class TabletData;
}
namespace indexlib::index {
class MultiFieldIndexReader;
}
namespace indexlibv2::table {
class IndexReclaimer;
class IndexReclaimerParam;
} // namespace indexlibv2::table

namespace indexlibv2::table {

class IndexReclaimerCreator : private autil::NoCopyable
{
public:
    IndexReclaimerCreator(std::shared_ptr<config::ITabletSchema> tabletSchema,
                          std::shared_ptr<framework::TabletData> tabletData,
                          std::map<segmentid_t, docid_t> segmentId2BaseDocId,
                          std::map<segmentid_t, size_t> segmentId2DocCount,
                          const std::shared_ptr<kmonitor::MetricsReporter>& reporter);
    ~IndexReclaimerCreator();

public:
    IndexReclaimer* Create(const IndexReclaimerParam& param) const;

private:
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
    std::shared_ptr<framework::TabletData> _tabletData;
    std::map<segmentid_t, docid_t> _segmentId2BaseDocId;
    std::map<segmentid_t, size_t> _segmentId2DocCount;
    std::shared_ptr<indexlib::index::MultiFieldIndexReader> _multiFieldIndexReader;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
