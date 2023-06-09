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
#ifndef __INDEXLIB_INDEX_FIELD_RECLAIMER_H
#define __INDEXLIB_INDEX_FIELD_RECLAIMER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/document_reclaimer/document_deleter.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer.h"

namespace indexlib { namespace merger {

class IndexFieldReclaimer : public IndexReclaimer
{
public:
    IndexFieldReclaimer(const index_base::PartitionDataPtr& partitionData, const IndexReclaimerParam& reclaimParam,
                        util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    ~IndexFieldReclaimer();

public:
    bool Reclaim(const DocumentDeleterPtr& docDeleter) override;
    bool Init(const config::IndexConfigPtr& indexConfig, const index::legacy::MultiFieldIndexReaderPtr& indexReaders);

private:
    std::vector<std::string> mTerms;
    config::IndexConfigPtr mIndexConfig;
    IE_DECLARE_METRIC(termReclaim);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexFieldReclaimer);
}} // namespace indexlib::merger

#endif //__INDEXLIB_INDEX_FIELD_RECLAIMER_H
