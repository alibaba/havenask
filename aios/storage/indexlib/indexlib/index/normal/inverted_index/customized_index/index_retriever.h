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
#ifndef __INDEXLIB_INDEX_RETRIEVER_H
#define __INDEXLIB_INDEX_RETRIEVER_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, IndexSegmentRetriever);

namespace indexlib { namespace index {
class IndexRetriever
{
public:
    IndexRetriever();
    virtual ~IndexRetriever();

public:
    virtual bool Init(const DeletionMapReaderPtr& deletionMapReader,
                      const std::vector<IndexSegmentRetrieverPtr>& retrievers,
                      const IndexerResourcePtr& resource = IndexerResourcePtr());

    virtual index::Result<std::vector<SegmentMatchInfo>> Search(const index::Term& term, autil::mem_pool::Pool* pool);

    virtual void SearchAsync(const index::Term& term, autil::mem_pool::Pool* pool,
                             std::function<void(bool, std::vector<SegmentMatchInfo>)> done);

protected:
    index::DeletionMapReaderPtr mDeletionMapReader;
    std::vector<IndexSegmentRetrieverPtr> mSegRetrievers;
    IndexerResourcePtr mIndexerResource;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexRetriever);
}} // namespace indexlib::index

#endif //__INDEXLIB_INDEX_RETRIEVER_H
