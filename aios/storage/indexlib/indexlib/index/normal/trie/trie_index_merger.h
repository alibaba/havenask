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
#include "indexlib/index/normal/framework/index_merger.h"
#include "indexlib/index/normal/framework/index_merger_creator.h"
#include "indexlib/index/normal/trie/double_array_trie.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(file_system, FileReader);

namespace indexlib { namespace index {

class TrieIndexMerger : public IndexMerger
{
private:
    typedef DoubleArrayTrie::KVPairVector KVPairVector;
    typedef std::map<autil::StringView, docid_t> KVMap;

public:
    TrieIndexMerger();
    ~TrieIndexMerger();

public:
    DECLARE_INDEX_MERGER_IDENTIFIER(trie);
    DECLARE_INDEX_MERGER_CREATOR(TrieIndexMerger, it_trie);

public:
    void Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        return InnerMerge(resource, segMergeInfos, outputSegMergeInfos);
    }

    void SortByWeightMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        return Merge(resource, segMergeInfos, outputSegMergeInfos);
    }

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

private:
    void InnerMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);
    void MergeRawData(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                      std::vector<KVPairVector>& sortedVectors);

    void ReadSegment(const index_base::SegmentData& segmentData, const index::MergerResource& resource,
                     std::vector<KVPairVector>& kvMap);

    size_t ReadData(const file_system::FileReaderPtr& fileReader, size_t cursor, size_t length, void* buffer) const;

private:
    autil::mem_pool::Pool mPool; // TODO: consider
    util::SimplePool mSimplePool;

private:
    friend class TrieIndexMergerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TrieIndexMerger);
}} // namespace indexlib::index
