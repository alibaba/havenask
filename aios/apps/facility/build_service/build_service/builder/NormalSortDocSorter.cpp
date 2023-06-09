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
#include "build_service/builder/NormalSortDocSorter.h"

#include "build_service/builder/DocumentMerger.h"

using namespace std;

namespace build_service { namespace builder {
BS_LOG_SETUP(builder, NormalSortDocSorter);

class DocumentComparatorWrapper1
{
public:
    typedef std::vector<SortDocument> DocumentVector;

public:
    DocumentComparatorWrapper1(const DocumentVector& documentVec) : _documentVec(documentVec) {}
    ~DocumentComparatorWrapper1() {}

public:
    inline bool operator()(uint32_t left, uint32_t right)
    {
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
    }

private:
    const DocumentVector& _documentVec;
    BS_LOG_DECLARE();
};

void NormalSortDocSorter::sort(std::vector<uint32_t>& sortDocVec)
{
    std::vector<uint32_t> nonSortDocVec;
    DocumentMerger documentMerger(_documentVec, _converter, _hasSub, _hasInvertedIndexUpdate);
    DocumentPkPosMap::iterator iter = _documentPkPosMap->begin();
    DocumentPkPosMap::iterator endIter = _documentPkPosMap->end();
    for (; iter != endIter; ++iter) {
        int32_t firstPos = iter->second.first;
        int32_t restPos = iter->second.second;

        if (_documentVec[firstPos]._docType == ADD_DOC) {
            sortDocVec.push_back(firstPos);
        } else {
            documentMerger.merge(firstPos);
        }
        if (restPos != -1) {
            PosVector::iterator it = (*_mergePosVector)[restPos].begin();
            PosVector::iterator endIt = (*_mergePosVector)[restPos].end();
            for (; it != endIt; ++it) {
                documentMerger.merge(*it);
            }
        }
        const vector<uint32_t>& mergedPos = documentMerger.getResult();
        nonSortDocVec.insert(nonSortDocVec.end(), mergedPos.begin(), mergedPos.end());
        documentMerger.clear();
    }
    std::sort(sortDocVec.begin(), sortDocVec.end(), DocumentComparatorWrapper1(_documentVec));
    sortDocVec.insert(sortDocVec.end(), nonSortDocVec.begin(), nonSortDocVec.end());
}

}} // namespace build_service::builder
