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
#include "ha3/search/PartialIndexPartitionReaderWrapper.h"

#include <algorithm>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"

namespace indexlib {
namespace util {
class Term;
} // namespace util
} // namespace indexlib

using namespace isearch::common;
using namespace std;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, PartialIndexPartitionReaderWrapper);

PartialIndexPartitionReaderWrapper::PartialIndexPartitionReaderWrapper(
    const std::map<std::string, uint32_t> *indexName2IdMap,
    const std::map<std::string, uint32_t> *attrName2IdMap,
    const std::vector<IndexPartitionReaderPtr> *indexReaderVec,
    bool ownPointer)
    : IndexPartitionReaderWrapper(indexName2IdMap, attrName2IdMap, indexReaderVec, ownPointer) {}

PartialIndexPartitionReaderWrapper::PartialIndexPartitionReaderWrapper(
    const std::shared_ptr<indexlibv2::framework::ITabletReader> &tabletReader)
    : IndexPartitionReaderWrapper(tabletReader) {}

void PartialIndexPartitionReaderWrapper::getTermKeyStr(const Term &term,
                                                       const LayerMeta *layerMeta,
                                                       string &keyStr) {
    IndexPartitionReaderWrapper::getTermKeyStr(term, layerMeta, keyStr);
    if (NULL != layerMeta) {
        keyStr.append(1, '\t');
        keyStr += layerMeta->getRangeString();
    }
}

indexlib::index::PostingIterator *PartialIndexPartitionReaderWrapper::doLookupByRanges(
    const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReaderPtr,
    const indexlib::index::Term *indexTerm,
    PostingType pt1,
    PostingType pt2,
    const LayerMeta *layerMeta,
    bool isSubIndex) {
    if (NULL == layerMeta) {
        return IndexPartitionReaderWrapper::doLookupByRanges(
            indexReaderPtr, indexTerm, pt1, pt2, layerMeta, isSubIndex);
    }
    if (0 == layerMeta->size()) {
        return NULL;
    }
    indexlib::DocIdRangeVector ranges;
    if (!isSubIndex) {
        for (auto &docIdRangeMeta : *layerMeta) {
            ranges.emplace_back(docIdRangeMeta.begin, docIdRangeMeta.end + 1);
        }
    } else {
        if (!getSubRanges(layerMeta, ranges)) {
            AUTIL_LOG(WARN, "layerMeta:[%s] get sub ranges failed.", layerMeta->toString().c_str());
            return IndexPartitionReaderWrapper::doLookupByRanges(
                indexReaderPtr, indexTerm, pt1, pt2, NULL, isSubIndex);
        }
    }

    PostingIterator *iter
        = indexReaderPtr->PartialLookup(*indexTerm, ranges, _topK, pt1, _sessionPool)
              .ValueOrThrow();
    if (!iter) {
        iter = indexReaderPtr->PartialLookup(*indexTerm, ranges, _topK, pt2, _sessionPool)
                   .ValueOrThrow();
    }
    return iter;
}

} // namespace search
} // namespace isearch
