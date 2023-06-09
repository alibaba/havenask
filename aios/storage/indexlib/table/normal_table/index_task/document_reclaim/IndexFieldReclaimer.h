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
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexReclaimer.h"
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexReclaimerParam.h"

namespace indexlibv2::framework {
class TabletData;
}
namespace indexlib::config {
class IndexConfig;
}
namespace indexlibv2::table {
class IndexReclaimerParam;
}
namespace indexlibv2::table {

class IndexFieldReclaimer : public IndexReclaimer
{
public:
    IndexFieldReclaimer(const std::shared_ptr<kmonitor::MetricsReporter>& reporter, const IndexReclaimerParam& param,
                        std::map<segmentid_t, docid_t> segmentId2BaseDocId,
                        std::map<segmentid_t, size_t> segmentId2DocCount);
    ~IndexFieldReclaimer() = default;

public:
    Status Reclaim(index::DeletionMapPatchWriter* docDeleter) override;
    bool Init(const std::shared_ptr<indexlib::index::MultiFieldIndexReader>& multiFieldIndexReader) override;

private:
    IndexReclaimerParam _param;
    std::map<segmentid_t, docid_t> _segmentId2BaseDocId;
    std::map<segmentid_t, size_t> _segmentId2DocCount;
    std::shared_ptr<indexlib::index::MultiFieldIndexReader> _multiFieldIndexReader;
    std::shared_ptr<indexlib::config::IndexConfig> _indexConfig;
    INDEXLIB_FM_DECLARE_METRIC(termReclaim);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
