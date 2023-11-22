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
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_user_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib { namespace index {

class IndexSegmentRetriever
{
public:
    IndexSegmentRetriever(const util::KeyValueMap& parameters) : mParameters(parameters) {};

    virtual ~IndexSegmentRetriever() {}

public:
    bool Open(const IndexerSegmentData& segData);

    virtual MatchInfo Search(const std::string& query, autil::mem_pool::Pool* sessionPool) = 0;

    virtual size_t EstimateLoadMemoryUse(const file_system::DirectoryPtr& indexDir) const = 0;

    const IndexerSegmentData& GetIndexerSegmentData() const { return mIndexerSegmentData; }

protected:
    virtual bool DoOpen(const file_system::DirectoryPtr& indexDir) = 0;

protected:
    util::KeyValueMap mParameters;
    IndexerSegmentData mIndexerSegmentData;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexSegmentRetriever);
}} // namespace indexlib::index
