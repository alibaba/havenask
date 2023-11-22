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

#include <cstdint>
#include <functional>
#include <memory>
#include <stddef.h>
#include <tr1/functional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/Span.h"
#include "autil/mem_pool/PoolVector.h"
#include "autil/mem_pool/pool_allocator.h"
#include "build_service/builder/SortDocumentConverter.h"
#include "build_service/builder/SortDocumentSorter.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace builder {

struct KVSimpleHash {
    size_t operator()(const autil::StringView& constString) const
    {
        return std::tr1::_Fnv_hash::hash(constString.data(), constString.size());
    }
};

class KVSortDocSorter : public SortDocumentSorter
{
public:
    typedef autil::mem_pool::pool_allocator<std::pair<const autil::StringView, int32_t>> _map_allocator_type;
    typedef std::unordered_map<autil::StringView, int32_t, KVSimpleHash, std::equal_to<autil::StringView>,
                               _map_allocator_type>
        DocumentPkPosMap;
    typedef autil::mem_pool::PoolVector<uint32_t> PosVector;
    typedef autil::mem_pool::PoolVector<PosVector> MergePosVector;
    typedef autil::mem_pool::PoolVector<uint32_t> UnsortedVector;

public:
    KVSortDocSorter(std::vector<SortDocument>& documentVec);
    ~KVSortDocSorter() {}

public:
    void push(const SortDocument& sortDoc, uint32_t pos) override;
    void sort(std::vector<uint32_t>& sortDocVec) override;

private:
    KVSortDocSorter(const KVSortDocSorter&);
    KVSortDocSorter& operator=(const KVSortDocSorter&);

private:
    std::vector<SortDocument>& _documentVec;
    std::unique_ptr<DocumentPkPosMap> _documentPkPosMap;
    std::unique_ptr<MergePosVector> _mergePosVector;
    std::unique_ptr<UnsortedVector> _unsortVector;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(KVSortDocSorter);

}} // namespace build_service::builder
