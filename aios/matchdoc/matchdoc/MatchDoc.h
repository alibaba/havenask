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
#ifndef ISEARCH_MATCHDOC_H
#define ISEARCH_MATCHDOC_H

#include <stdint.h>
#include "autil/DataBuffer.h"
#include "matchdoc/Trait.h"

namespace matchdoc {

class MatchDoc {
private:
    static const uint32_t MASK = 0x80000000;
    static const uint32_t MAX_INDEX = 0x7fffffff;
public:
    explicit MatchDoc(uint32_t index_ = MAX_INDEX,
                      int32_t docid_ = -1)
        : index(index_)
        , docid(docid_)
    {
    }
    bool operator == (const MatchDoc &rhs) const {
        return getIndex() == rhs.getIndex();
    }
    bool operator != (const MatchDoc &rhs) const {
        return !(*this == rhs);
    }
    int32_t getDocId() const { return docid; }
    void setDocId(int32_t docid) { this->docid = docid; }

    void setDeleted() {
        index |= MASK;
    }
    bool isDeleted() const {
        return (index & MASK) != 0;
    }

    uint32_t getIndex() const {
        return index & (~MASK);
    }
    int64_t toInt64() const {
        return *reinterpret_cast<const int64_t*>(this);
    }
    void fromInt64(int64_t i) {
        *reinterpret_cast<int64_t*>(this) = i;
    }
private:
    uint32_t index;
    int32_t docid;
};

template<>
struct SupportSerializeTrait<MatchDoc> {
    typedef UnSupportSerializeType type;
};


extern MatchDoc INVALID_MATCHDOC;

inline bool operator < (MatchDoc doc1, MatchDoc doc2) {
    return doc1.getIndex() < doc2.getIndex();
}

}
DECLARE_NO_NEED_DESTRUCT_TYPE(matchdoc::MatchDoc);
DECLARE_CAN_MOUNT_TYPE(matchdoc::MatchDoc);

#endif //ISEARCH_MATCHDOC_H
