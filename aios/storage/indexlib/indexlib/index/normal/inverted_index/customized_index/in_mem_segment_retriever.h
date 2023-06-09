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
#ifndef __INDEXLIB_IN_MEM_SEGMENT_RETRIEVER_H
#define __INDEXLIB_IN_MEM_SEGMENT_RETRIEVER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

class InMemSegmentRetriever : public IndexSegmentRetriever
{
public:
    InMemSegmentRetriever(const util::KeyValueMap& parameters) : IndexSegmentRetriever(parameters) {}

    ~InMemSegmentRetriever() {}

public:
    bool DoOpen(const file_system::DirectoryPtr& indexDir) override
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "not support call Open interface!");
        return false;
    }

    size_t EstimateLoadMemoryUse(const file_system::DirectoryPtr& indexDir) const override
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "not support call EstimateLoadMemoryUse interface!");
        return 0;
    }

    void SetIndexerSegmentData(const IndexerSegmentData& indexSegData) { mIndexerSegmentData = indexSegData; }

    // TODO: user should override Search() interface below
    /*
      MatchInfo Search(const std::string& query, autil::mem_pool::Pool* sessionPool) override;
    */

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemSegmentRetriever);
}} // namespace indexlib::index

#endif //__INDEXLIB_IN_MEM_SEGMENT_RETRIEVER_H
