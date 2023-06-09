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
#ifndef ISEARCH_BS_KKVSORTDOCSORTER_H
#define ISEARCH_BS_KKVSORTDOCSORTER_H

#include <unordered_map>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolVector.h"
#include "autil/mem_pool/pool_allocator.h"
#include "build_service/builder/SortDocumentSorter.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/index/kkv/kkv_define.h"

namespace build_service { namespace builder {

class KKVSortDocSorter : public SortDocumentSorter
{
public:
    struct PosNode {
        uint64_t skey;
        uint32_t pos;
    };

public:
    typedef autil::mem_pool::pool_allocator<std::pair<const indexlib::index::PKeyType, int32_t>> _map_allocator_type;
    typedef std::unordered_map<indexlib::index::PKeyType, int32_t, std::hash<indexlib::index::PKeyType>,
                               std::equal_to<indexlib::index::PKeyType>, _map_allocator_type>
        DocumentPkPosMap;
    typedef autil::mem_pool::PoolVector<PosNode> PosVector;
    typedef autil::mem_pool::PoolVector<PosVector> MergePosVector;
    typedef autil::mem_pool::PoolVector<uint32_t> UnsortedVector;

public:
    KKVSortDocSorter();
    ~KKVSortDocSorter() {}

public:
    void push(const SortDocument& sortDoc, uint32_t pos) override
    {
        assert(sortDoc._kvDoc->GetDocumentCount() == 1u);
        auto& indexDoc = sortDoc._kvDoc->back();
        indexlib::index::PKeyType pkey = indexDoc.GetPKeyHash();
        DocumentPkPosMap::iterator iter = _documentPkPosMap->find(pkey);

        if (indexDoc.GetDocOperateType() == DELETE_DOC && !indexDoc.HasSKey()) {
            if (iter != _documentPkPosMap->end()) {
                (*_mergePosVector)[iter->second].clear();
            }
            _unsortVector->push_back(pos);
            return;
        }

        PosNode node;
        node.skey = indexDoc.GetSKeyHash();
        node.pos = pos;
        if (iter == _documentPkPosMap->end()) {
            PosVector tmpVec(_pool.get());
            tmpVec.push_back(node);
            (*_mergePosVector).push_back(tmpVec);
            uint32_t idx = (*_mergePosVector).size() - 1;
            (*_documentPkPosMap)[pkey] = idx;
            return;
        }
        (*_mergePosVector)[iter->second].push_back(node);
    }
    void sort(std::vector<uint32_t>& sortDocVec) override;

private:
    KKVSortDocSorter(const KKVSortDocSorter&);
    KKVSortDocSorter& operator=(const KKVSortDocSorter&);

private:
    std::unique_ptr<DocumentPkPosMap> _documentPkPosMap;
    std::unique_ptr<MergePosVector> _mergePosVector;
    std::unique_ptr<UnsortedVector> _unsortVector;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(KKVSortDocSorter);

}} // namespace build_service::builder

#endif // ISEARCH_BS_KKVSORTDOCSORTER_H
