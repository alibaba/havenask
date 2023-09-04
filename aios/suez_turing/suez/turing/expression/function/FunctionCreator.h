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

#include "autil/Log.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/FunctionCreatorResource.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/function/FunctionMap.h"

namespace suez {
namespace turing {

#define FUNCTION_CREATOR_CLASS_NAME(classname) classname##Creator
#define DECLARE_FUNCTION_CREATOR(classname, funcName, argCount, ...)                                                   \
    class FUNCTION_CREATOR_CLASS_NAME(classname) : public suez::turing::FunctionCreator {                              \
    public:                                                                                                            \
        suez::turing::FunctionProtoInfo getFuncProtoInfo() const {                                                     \
            return suez::turing::FunctionProtoInfo(                                                                    \
                argCount, classname::getType(), classname::isMultiValue(), ##__VA_ARGS__);                             \
        }                                                                                                              \
        std::string getFuncName() const { return funcName; }                                                           \
    }

#define DECLARE_TEMPLATE_FUNCTION_CREATOR(classname, T, funcName, argCount, ...)                                       \
    class FUNCTION_CREATOR_CLASS_NAME(classname) : public suez::turing::FunctionCreator {                              \
    public:                                                                                                            \
        suez::turing::FunctionProtoInfo getFuncProtoInfo() const {                                                     \
            return suez::turing::FunctionProtoInfo(argCount,                                                           \
                                                   suez::turing::TypeHaInfoTraits<T>::VARIABLE_TYPE,                   \
                                                   suez::turing::TypeHaInfoTraits<T>::IS_MULTI,                        \
                                                   ##__VA_ARGS__);                                                     \
        }                                                                                                              \
        std::string getFuncName() const { return funcName; }                                                           \
    }

#define DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(classname, funcName, argCount, ...)                                      \
    class FUNCTION_CREATOR_CLASS_NAME(classname) : public suez::turing::FunctionCreator {                              \
    public:                                                                                                            \
        suez::turing::FunctionProtoInfo getFuncProtoInfo() const {                                                     \
            return suez::turing::FunctionProtoInfo(argCount, vt_unknown, false, ##__VA_ARGS__);                        \
        }                                                                                                              \
        std::string getFuncName() const { return funcName; }                                                           \
    }

#define DECLARE_MAX_TYPE_FUNCTION_CREATOR(classname, funcName, argCount, ...)                                          \
    class FUNCTION_CREATOR_CLASS_NAME(classname) : public suez::turing::FunctionCreator {                              \
    public:                                                                                                            \
        suez::turing::FunctionProtoInfo getFuncProtoInfo() const {                                                     \
            return suez::turing::FunctionProtoInfo(argCount, vt_unknown_max_type, false, ##__VA_ARGS__);               \
        }                                                                                                              \
        std::string getFuncName() const { return funcName; }                                                           \
    }

class FunctionCreator {
public:
    FunctionCreator() {}
    virtual ~FunctionCreator() {}

private:
    FunctionCreator(const FunctionCreator &);
    FunctionCreator &operator=(const FunctionCreator &);

public:
    virtual bool init(const KeyValueMap &funcParameter, const suez::ResourceReaderPtr &resourceReader) { return true; }

    virtual bool init(const KeyValueMap &funcParameter, const FunctionCreatorResource &funcCreatorResource) {
        // default implementation
        return init(funcParameter, funcCreatorResource.resourceReader);
    }

    virtual suez::turing::FunctionProtoInfo getFuncProtoInfo() const = 0;
    virtual FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) = 0;
    // after clone it will call init
    virtual FunctionCreator *clone() { return NULL; }

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(FunctionCreator);

} // namespace turing
} // namespace suez
