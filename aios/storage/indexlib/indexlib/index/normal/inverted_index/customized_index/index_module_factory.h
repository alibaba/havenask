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

#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_user_data.h"
#include "indexlib/plugin/ModuleFactory.h"

namespace indexlib { namespace index {

class IndexModuleFactory : public plugin::ModuleFactory
{
public:
    IndexModuleFactory() {};
    ~IndexModuleFactory() {};

public:
    virtual void destroy() = 0;
    virtual Indexer* createIndexer(const util::KeyValueMap& parameters) = 0;
    virtual IndexReducer* createIndexReducer(const util::KeyValueMap& parameters) = 0;
    virtual IndexSegmentRetriever* createIndexSegmentRetriever(const util::KeyValueMap& parameters) = 0;
    virtual IndexRetriever* createIndexRetriever(const util::KeyValueMap& parameters) { return new IndexRetriever(); }

    virtual IndexerUserData* createIndexerUserData(const util::KeyValueMap& parameters)
    {
        return new IndexerUserData();
    }
};

DEFINE_SHARED_PTR(IndexModuleFactory);
}} // namespace indexlib::index
