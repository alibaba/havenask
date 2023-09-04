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

#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlibv2::framework {
class ITabletReader;
}

namespace indexlib {
namespace util {
class Term;
} // namespace util
} // namespace indexlib
namespace isearch {
namespace common {
class Term;
} // namespace common
namespace search {
class LayerMeta;
} // namespace search
} // namespace isearch

namespace isearch {
namespace search {

class PartialIndexPartitionReaderWrapper : public IndexPartitionReaderWrapper {
public:
    PartialIndexPartitionReaderWrapper(const std::map<std::string, uint32_t> *indexName2IdMap,
                                       const std::map<std::string, uint32_t> *attrName2IdMap,
                                       const std::vector<IndexPartitionReaderPtr> *indexReaderVec,
                                       bool ownPointer);

    PartialIndexPartitionReaderWrapper(
        const std::shared_ptr<indexlibv2::framework::ITabletReader> &tabletReader);

private:
    PartialIndexPartitionReaderWrapper(const PartialIndexPartitionReaderWrapper &);
    PartialIndexPartitionReaderWrapper &operator=(const PartialIndexPartitionReaderWrapper &);

protected:
    void getTermKeyStr(const common::Term &term,
                       const LayerMeta *layerMeta,
                       std::string &keyStr) override;
    PostingIterator *
    doLookupByRanges(const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReaderPtr,
                     const indexlib::index::Term *indexTerm,
                     PostingType pt1,
                     PostingType pt2,
                     const LayerMeta *layerMeta,
                     bool isSubIndex) override;

private:
    friend class PartialIndexPartitionReaderWrapperTest;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PartialIndexPartitionReaderWrapper> PartialIndexPartitionReaderWrapperPtr;

} // namespace search
} // namespace isearch
