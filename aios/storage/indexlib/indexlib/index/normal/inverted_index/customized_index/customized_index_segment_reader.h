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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/plugin_manager.h"

namespace indexlib { namespace index {

class CustomizedIndexSegmentReader : public IndexSegmentReader
{
public:
    CustomizedIndexSegmentReader();
    ~CustomizedIndexSegmentReader();

public:
    bool Init(const config::IndexConfigPtr& indexConfig, const IndexSegmentRetrieverPtr& retriever);

    const IndexSegmentRetrieverPtr& GetSegmentRetriever() const { return mRetriever; }

    bool GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool,
                           InvertedIndexSearchTracer* tracer = nullptr) const override;

private:
    IndexSegmentRetrieverPtr mRetriever;
    config::IndexConfigPtr mIndexConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexSegmentReader);
}} // namespace indexlib::index
