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
#ifndef ISEARCH_BS_NORMALSORTDOCSORTER_H
#define ISEARCH_BS_NORMALSORTDOCSORTER_H

#include <tr1/functional>
#include <unordered_map>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolVector.h"
#include "autil/mem_pool/pool_allocator.h"
#include "build_service/builder/NormalSortDocConvertor.h"
#include "build_service/builder/SortDocumentSorter.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace builder {

struct SimpleHash {
    size_t operator()(const autil::StringView& constString) const
    {
        return std::tr1::_Fnv_hash::hash(constString.data(), constString.size());
    }
};

class NormalSortDocSorter : public SortDocumentSorter
{
public:
    typedef autil::mem_pool::pool_allocator<std::pair<const autil::StringView, std::pair<int32_t, int32_t>>>
        _map_allocator_type;
    typedef std::unordered_map<autil::StringView, std::pair<int32_t, int32_t>, SimpleHash,
                               std::equal_to<autil::StringView>, _map_allocator_type>
        DocumentPkPosMap;
    typedef autil::mem_pool::PoolVector<uint32_t> PosVector;
    typedef autil::mem_pool::PoolVector<PosVector> MergePosVector;

public:
    NormalSortDocSorter(std::vector<SortDocument>& documentVec, NormalSortDocConvertor* converter, bool hasSub,
                        bool hasInvertedIndexUpdate)
        : _documentVec(documentVec)
        , _converter(converter)
        , _documentPkPosMap(new DocumentPkPosMap(10, DocumentPkPosMap::hasher(), DocumentPkPosMap::key_equal(),
                                                 _map_allocator_type(_pool.get())))
        , _mergePosVector(new MergePosVector(_pool.get()))
        , _hasSub(hasSub)
        , _hasInvertedIndexUpdate(hasInvertedIndexUpdate)
    {
    }

    ~NormalSortDocSorter() {}

private:
    NormalSortDocSorter(const NormalSortDocSorter&);
    NormalSortDocSorter& operator=(const NormalSortDocSorter&);

public:
    void push(const SortDocument& sortDoc, uint32_t pos) override
    {
        const __typeof__(sortDoc._pk)& pkStr = sortDoc._pk;
        DocumentPkPosMap::iterator iter = _documentPkPosMap->find(pkStr);

        if (iter == _documentPkPosMap->end()) {
            (*_documentPkPosMap)[pkStr] = std::make_pair(pos, -1);
            return;
        }

        if (sortDoc._docType == ADD_DOC) {
            iter->second.first = pos;
            iter->second.second = -1;
        } else {
            if (iter->second.second != -1) {
                (*_mergePosVector)[iter->second.second].push_back(pos);
            } else {
                PosVector tmpVec(_pool.get());
                tmpVec.push_back(pos);
                (*_mergePosVector).push_back(tmpVec);
                iter->second.second = (*_mergePosVector).size() - 1;
            }
        }
    }

    void sort(std::vector<uint32_t>& sortDocVec) override;

private:
    std::vector<SortDocument>& _documentVec;
    NormalSortDocConvertor* _converter;
    std::unique_ptr<DocumentPkPosMap> _documentPkPosMap;
    std::unique_ptr<MergePosVector> _mergePosVector;
    bool _hasSub;
    bool _hasInvertedIndexUpdate;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(NormalSortDocSorter);

}} // namespace build_service::builder

#endif // ISEARCH_BS_NORMALSORTDOCSORTER_H
