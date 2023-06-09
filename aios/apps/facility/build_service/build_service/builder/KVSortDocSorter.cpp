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
#include "build_service/builder/KVSortDocSorter.h"

using namespace std;

namespace build_service { namespace builder {
BS_LOG_SETUP(builder, KVSortDocSorter);

KVSortDocSorter::KVSortDocSorter(std::vector<SortDocument>& documentVec)
    : _documentVec(documentVec)
    , _documentPkPosMap(new DocumentPkPosMap(10, DocumentPkPosMap::hasher(), DocumentPkPosMap::key_equal(),
                                             _map_allocator_type(_pool.get())))
    , _mergePosVector(new MergePosVector(_pool.get()))
    , _unsortVector(new UnsortedVector(_pool.get()))
{
}

void KVSortDocSorter::sort(std::vector<uint32_t>& sortDocVec)
{
    for (size_t i = 0; i < _unsortVector->size(); i++) {
        sortDocVec.push_back((*_unsortVector)[i]);
    }

    vector<uint32_t> idxVec;
    idxVec.reserve(_mergePosVector->size());
    for (size_t i = 0; i < _mergePosVector->size(); i++) {
        if ((*_mergePosVector)[i].empty()) {
            continue;
        }
        idxVec.push_back(i);
    }

    std::sort(idxVec.begin(), idxVec.end(), [this](uint32_t lft, uint32_t rht) -> bool {
        uint32_t left = *(*_mergePosVector)[lft].rbegin();
        uint32_t right = *(*_mergePosVector)[rht].rbegin();
        const SortDocument& leftDoc = _documentVec[left];
        const SortDocument& rightDoc = _documentVec[right];
        assert(leftDoc._sortKey.size() == rightDoc._sortKey.size());
        int r = memcmp(leftDoc._sortKey.data(), rightDoc._sortKey.data(), rightDoc._sortKey.size());
        if (r < 0) {
            return true;
        } else if (r > 0) {
            return false;
        } else {
            return left < right;
        }
    });

    for (auto& idx : idxVec) {
        for (auto& pos : (*_mergePosVector)[idx]) {
            sortDocVec.push_back(pos);
        }
    }
}

void KVSortDocSorter::push(const SortDocument& sortDoc, uint32_t pos)
{
    DocumentPkPosMap::iterator iter = _documentPkPosMap->find(sortDoc._pk);
    if (sortDoc._docType == DELETE_DOC) {
        if (iter != _documentPkPosMap->end()) {
            (*_mergePosVector)[iter->second].clear();
        }
        _unsortVector->push_back(pos);
        return;
    }

    assert(sortDoc._docType == ADD_DOC);
    if (iter == _documentPkPosMap->end()) {
        PosVector tmpVec(_pool.get());
        tmpVec.push_back(pos);
        (*_mergePosVector).push_back(tmpVec);
        uint32_t idx = (*_mergePosVector).size() - 1;
        (*_documentPkPosMap)[sortDoc._pk] = idx;
        return;
    }
    (*_mergePosVector)[iter->second].push_back(pos);
}

}} // namespace build_service::builder
