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
#ifndef __INDEXLIB_AND_INDEX_RECLAIMER_H
#define __INDEXLIB_AND_INDEX_RECLAIMER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer.h"

DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(index, InvertedIndexReader);
namespace indexlib { namespace merger {

class AndIndexReclaimer : public IndexReclaimer
{
public:
    AndIndexReclaimer(const index_base::PartitionDataPtr& partitionData, const IndexReclaimerParam& reclaimParam,
                      util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    ~AndIndexReclaimer();

public:
    bool Reclaim(const DocumentDeleterPtr& docDeleter) override;
    bool Init(const config::IndexSchemaPtr& indexSchema, const index::legacy::MultiFieldIndexReaderPtr& indexReaders);

private:
    IE_DECLARE_METRIC(AndReclaim);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AndIndexReclaimer);
}} // namespace indexlib::merger

#endif //__INDEXLIB_AND_INDEX_RECLAIMER_H
