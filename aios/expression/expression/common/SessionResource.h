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
#ifndef ISEARCH_EXPRESSION_SESSIONRESOURCE_H
#define ISEARCH_EXPRESSION_SESSIONRESOURCE_H

#include "expression/common.h"
#include "autil/mem_pool/Pool.h"
#include "autil/NoCopyable.h"
#include "autil/DataBuffer.h"
#include "matchdoc/MatchDocAllocator.h"
#include <rapidjson/document.h>

namespace expression {

enum FuncLocation {
    FL_UNKNOWN = 1,
    FL_SEARCHER = 2,
    FL_PROXY = 4
};

struct SortMeta {
    std::string name;
    matchdoc::ReferenceBase *ref;
    bool isAsc;
    SortMeta()
        : ref(NULL)
        , isAsc(true)
    {
    }
    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(name);
        dataBuffer.write(isAsc);
    }
    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(name);
        dataBuffer.read(isAsc);
    }
    bool operator==(const SortMeta &other) const {
        if (this == &other) {
            return true;
        }
        return name == other.name
            && ref == other.ref
            && isAsc == other.isAsc;
    }
    bool operator!=(const SortMeta &other) const {
        return !(*this == other);
    }
};


class SessionResource:public autil::NoCopyable , public autil::NoMoveable{
public:
    SessionResource()
        : sessionPool(NULL)
        , request(NULL)
        , location(FL_UNKNOWN)
        , sortMetas(NULL)
        , tableName("")
    {}
    SessionResource(const SessionResource &other)
        : sessionPool(other.sessionPool)
        , allocator(other.allocator)
        , request(other.request)
        , location(other.location)
        , sortMetas(other.sortMetas)
        , tableName(other.tableName)
    {}
    virtual ~SessionResource() {}

public:
    autil::mem_pool::Pool *sessionPool;
    matchdoc::MatchDocAllocatorPtr allocator;
    SimpleDocument *request;
    FuncLocation location;
    std::vector<SortMeta> *sortMetas;
    std::string tableName;
};
}

#endif //ISEARCH_EXPRESSION_SESSIONRESOURCE_H
