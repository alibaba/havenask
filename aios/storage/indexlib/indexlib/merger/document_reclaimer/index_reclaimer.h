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
#ifndef __INDEXLIB_INDEX_RECLAIMER_H
#define __INDEXLIB_INDEX_RECLAIMER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_param.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(merger, DocumentDeleter)

namespace indexlib { namespace merger {

class IndexReclaimer
{
public:
    IndexReclaimer(const index_base::PartitionDataPtr& partitionData, const IndexReclaimerParam& reclaimParam,
                   util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    virtual ~IndexReclaimer();

public:
    virtual bool Reclaim(const DocumentDeleterPtr& docDeleter) = 0;

protected:
    index_base::PartitionDataPtr mPartitionData;
    IndexReclaimerParam mReclaimParam;
    index::legacy::MultiFieldIndexReaderPtr mIndexReaders;
    util::MetricProviderPtr mMetricProvider;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexReclaimer);
}} // namespace indexlib::merger

#endif //__INDEXLIB_INDEX_RECLAIMER_H
