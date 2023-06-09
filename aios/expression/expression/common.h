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
#ifndef ISEARCH_EXPRESSION_COMMON_H
#define ISEARCH_EXPRESSION_COMMON_H

#include "autil/Log.h"
#include "autil/CommonMacros.h"
#include "autil/mem_pool/Pool.h"
#include <memory>

#ifndef RAPIDJSON_HAS_STDSTRING
#define RAPIDJSON_HAS_STDSTRING 1
#endif

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

enum ConstExprType {
    UNKNOWN,
    INTEGER_VALUE,
    FLOAT_VALUE,
    STRING_VALUE,
};

//enum SerializeLevel {
//    SL_NONE = 0,
//    SL_PROXY = 2,
//    SL_FRONTEND = 4
//};

namespace expression {

typedef int32_t docid_t;

class AutilPoolAllocator {
public:
    static const bool kNeedFree = false;

    AutilPoolAllocator(autil::mem_pool::Pool *pool_ = NULL)
    {
        if (pool_) {
            pool = pool_;
            ownPool = false;
        } else {
            pool = new autil::mem_pool::Pool;
            ownPool = true;
        }
    }
    ~AutilPoolAllocator() {
        if (ownPool) {
            DELETE_AND_SET_NULL(pool);
        }
    }
private:
    AutilPoolAllocator(const AutilPoolAllocator &);
    AutilPoolAllocator& operator=(const AutilPoolAllocator &);

public:
    void* Malloc(size_t size) {
        if (size == 0) {
            return NULL;
        } else {
            return pool->allocate(size);
        }
    }
    void* Realloc(void* originalPtr, size_t originalSize, size_t newSize) {
        // do nothing to old mem
        if (originalPtr == 0) {
            return Malloc(newSize);
        }
        if (newSize == 0) {
            return NULL;
        }
        if (originalSize >= newSize) {
            return originalPtr;
        }
        void* newBuffer = Malloc(newSize);
        RAPIDJSON_ASSERT(newBuffer != 0);
        if (originalSize)
            std::memcpy(newBuffer, originalPtr, originalSize);
        return newBuffer;
    }
    static void Free(void *ptr) {
        // do nothing to old mem
    }
public:
    autil::mem_pool::Pool *pool;
    bool ownPool;
};

typedef rapidjson::GenericValue<rapidjson::ASCII<>, expression::AutilPoolAllocator> SimpleValue;
typedef rapidjson::GenericDocument<rapidjson::ASCII<>, expression::AutilPoolAllocator> SimpleDocument;

#define EXPRESSION_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;

typedef std::map<std::string, std::string> KeyValueMap;

#define DECLARE_RESOURCE_READER                                         \
    namespace resource_reader {                                         \
    class ResourceReader;                                               \
    typedef std::shared_ptr<resource_reader::ResourceReader> ResourceReaderPtr; \
    }

typedef int32_t expressionid_t;
#define INVALID_EXPRESSION_ID expressionid_t(-1)
const std::string BUILD_IN_JOIN_DOCID_REF_PREIX = "_@_join_docid_";
const std::string PROVIDER_VAR_NAME_PREFIX = "_@_user_data_";

} // end namespace expression



#endif //ISEARCH_EXPRESSION_COMMON_H
