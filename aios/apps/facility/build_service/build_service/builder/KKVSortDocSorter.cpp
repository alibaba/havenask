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
#include "build_service/builder/KKVSortDocSorter.h"

using namespace std;

namespace build_service { namespace builder {
BS_LOG_SETUP(builder, KKVSortDocSorter);

class DocComparator
{
public:
    DocComparator() {}
    ~DocComparator() {}

public:
    inline bool operator()(const KKVSortDocSorter::PosNode& left, const KKVSortDocSorter::PosNode& right)
    {
        if (left.skey > right.skey) {
            return true;
        }

        if (left.skey < right.skey) {
            return false;
        }
        return left.pos < right.pos;
    }
};

KKVSortDocSorter::KKVSortDocSorter()
    : _documentPkPosMap(new DocumentPkPosMap(10, DocumentPkPosMap::hasher(), DocumentPkPosMap::key_equal(),
                                             _map_allocator_type(_pool.get())))
    , _mergePosVector(new MergePosVector(_pool.get()))
    , _unsortVector(new UnsortedVector(_pool.get()))
{
}

void KKVSortDocSorter::sort(std::vector<uint32_t>& sortDocVec)
{
    for (size_t i = 0; i < _unsortVector->size(); i++) {
        sortDocVec.push_back((*_unsortVector)[i]);
    }

    for (size_t i = 0; i < _mergePosVector->size(); i++) {
        if ((*_mergePosVector)[i].size() == 0) {
            continue;
        }
        std::sort((*_mergePosVector)[i].begin(), (*_mergePosVector)[i].end(), DocComparator());
        for (size_t j = 0; j < (*_mergePosVector)[i].size(); j++) {
            sortDocVec.push_back((*_mergePosVector)[i][j].pos);
        }
    }
}

}} // namespace build_service::builder
