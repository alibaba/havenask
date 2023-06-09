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

#include <stdint.h>
#include <memory>
#include <sstream>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/PoolVector.h"
#include "ha3/isearch.h"
#include "indexlib/indexlib.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace isearch {
namespace search {

struct DocIdRangeMeta {
    enum OrderedType {
        OT_ORDERED,
        OT_UNORDERED,
        OT_UNKNOWN
    };
    DocIdRangeMeta();
    DocIdRangeMeta(docid_t begin_, docid_t end_,
                   OrderedType ot = OT_UNKNOWN, uint32_t quota_ = 0);
    DocIdRangeMeta(const indexlib::DocIdRange& docIdRange,
                   OrderedType ot = OT_UNKNOWN, uint32_t quota_ = 0);

    bool operator != (const DocIdRangeMeta &other) const {
        return !(*this == other);
    }
    bool operator == (const DocIdRangeMeta &other) const {
        return begin == other.begin
            && end == other.end
            && nextBegin == other.nextBegin
            && quota == other.quota
            && ordered == other.ordered;
    }
    bool operator==(const DocIdRangeMeta& meta) {
        return begin == meta.begin
            && end == meta.end
            && nextBegin == meta.nextBegin
            && quota == meta.quota
            && ordered == meta.ordered;
    }

    docid_t nextBegin;
    docid_t end;
    uint32_t quota;
    docid_t begin;
    OrderedType ordered;
};

struct DocIdRangeMetaCompare {
    bool operator() (const DocIdRangeMeta& i,
                     const DocIdRangeMeta& j) {
        return i.begin < j.begin;
    }
};

///////////////////////////////////////////////////////////
std::ostream& operator << (std::ostream &os, const DocIdRangeMeta &range);

class LayerMeta : public autil::mem_pool::PoolVector<DocIdRangeMeta> {
public:
    LayerMeta(autil::mem_pool::Pool *pool);
    void initRangeString();
    std::string toString() const;
    std::string getRangeString() const;
public:
    uint32_t quota;
    uint32_t maxQuota;
    QuotaMode quotaMode;
    bool needAggregate;
    QuotaType quotaType;
private:
    std::string rangeString;
};

typedef autil::mem_pool::PoolVector<LayerMeta> LayerMetas;

typedef std::shared_ptr<LayerMetas> LayerMetasPtr;

typedef std::shared_ptr<LayerMeta> LayerMetaPtr;

} // namespace search
} // namespace isearch
