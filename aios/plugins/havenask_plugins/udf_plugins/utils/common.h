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

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "autil/mem_pool/PoolVector.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/ValueType.h"
#include "sql/common/Log.h"

constexpr char FIELDS_KEY[] = "input_output_fields";

#define BEGIN_HAVENASK_UDF_NAMESPACE(x)                                                                           \
    namespace havenask_udf {                                                                                         \
    namespace x {                                                                                                     
#define END_HAVENASK_UDF_NAMESPACE(x)                                                                             \
    }                                                                                                                  \
    }
#define USE_HAVENASK_UDF_NAMESPACE(x) using namespace havenask_udf::x;
#define HAVENASK_UDF_NS(x) havenask_udf::x


#define HAVENASK_UDF_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;

#define FRIEND_TEST(test_case_name, test_name) friend class test_case_name##_##test_name##_Test

#define FUNCTION_TEMPLATE_IMPLEMENT(FuncClassName)                                                                     \
    DECLARE_FUNCTION_CREATOR(FuncClassName, #FuncClassName, 0);                                                        \
    class FuncClassName##CreatorImpl : public FUNCTION_CREATOR_CLASS_NAME(FuncClassName) {                             \
    public:                                                                                                            \
        suez::turing::FunctionInterface *                                                                              \
        createFunction(const suez::turing::FunctionSubExprVec &funcSubExprVec) override {                              \
            if (!_funcPtr) {                                                                                           \
                _funcPtr.reset(new FuncClassName());                                                                   \
            }                                                                                                          \
            return _funcPtr->clone();                                                                                  \
        }                                                                                                              \
        bool init(const suez::turing::KeyValueMap &funcParameter,                                                      \
                  const suez::ResourceReaderPtr &resourceReader) override {                                            \
            _funcPtr.reset(new FuncClassName());                                                                       \
            if (!_funcPtr->init(funcParameter, resourceReader->getConfigPath())) {                                     \
                return false;                                                                                          \
            }                                                                                                          \
            return true;                                                                                               \
        }                                                                                                              \
        suez::turing::FunctionCreator *clone() override {                                                              \
            return new FuncClassName##CreatorImpl();                                                                   \
        }                                                                                                              \
                                                                                                                       \
    private:                                                                                                           \
        std::shared_ptr<FuncClassName> _funcPtr;                                                                       \
    };

BEGIN_HAVENASK_UDF_NAMESPACE(common)

static const std::string kTuringAliasPrefix = "__turing_alias__";

typedef autil::mem_pool::PoolVector<matchdoc::MatchDoc> MatchDocArray;
typedef int32_t MatchType;
static const MatchType kInvalidMatchType = std::numeric_limits<uint8_t>::max();

static const std::string kMatchTypeFieldName = "match_type";
static const std::string kMatchTypeListFieldName = "match_type_list";
static const std::string kReservedScoreFieldName = "__score__";

inline static std::string valueTypeToString(matchdoc::BuiltinType vt) {
    switch (vt) {
    case matchdoc::bt_int8:
        return "int8";
    case matchdoc::bt_uint8:
        return "uint8";
    case matchdoc::bt_int16:
        return "int16";
    case matchdoc::bt_uint16:
        return "uint16";
    case matchdoc::bt_int32:
        return "int32";
    case matchdoc::bt_uint32:
        return "uint32";
    case matchdoc::bt_int64:
        return "int64";
    case matchdoc::bt_uint64:
        return "uint64";
    case matchdoc::bt_float:
        return "float";
    case matchdoc::bt_double:
        return "double";
    case matchdoc::bt_string:
        return "string";
    default:
        return "unknown";
    }
}

inline static uint32_t xrand() {
    uint32_t time_high, time_low;
    __asm__(
            "rdtsc;"
            "movl %%edx,%0;"
            "movl %%eax,%1;"
            :"=m"(time_high),"=m"(time_low)
            :
            :"%edx","%eax"
           );
    return (time_high ^ time_low);
}

END_HAVENASK_UDF_NAMESPACE(common)

namespace havenask_udf {
typedef double score_t;
}
