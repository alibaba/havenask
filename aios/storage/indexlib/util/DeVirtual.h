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

#include <memory>

namespace indexlib { namespace util {

#define IE_SUB_CLASS_TYPE_NAME(classBase, subClass) classBase##_##subClass

/*********** macro used in base class *************/
#define IE_BASE_DECLARE_SUB_CLASS_BEGIN(classBase)                                                                     \
    class classBase;                                                                                                   \
    enum classBase##_SUBCLASS                                                                                          \
    {                                                                                                                  \
        classBase##_unknownType = 0,

#define IE_BASE_DECLARE_SUB_CLASS(classBase, subClass) IE_SUB_CLASS_TYPE_NAME(classBase, subClass),

#define IE_BASE_DECLARE_SUB_CLASS_END(classBase)                                                                       \
    }                                                                                                                  \
    ;                                                                                                                  \
    template <int type>                                                                                                \
    struct classBase##_SubClassTypeTraits {                                                                            \
        typedef classBase ClassType;                                                                                   \
    };

#define IE_BASE_CLASS_DECLARE(classBase)                                                                               \
protected:                                                                                                             \
    classBase##_SUBCLASS _objType = classBase##_unknownType;                                                           \
                                                                                                                       \
public:                                                                                                                \
    inline int getObjectType() const { return (int)_objType; }

/************** macro used in derived class **************/
#define IE_SUB_CLASS_TYPE_DECLARE(classBase, subClass)                                                                 \
    class subClass;                                                                                                    \
    template <>                                                                                                        \
    struct classBase##_SubClassTypeTraits<IE_SUB_CLASS_TYPE_NAME(classBase, subClass)> {                               \
        typedef subClass ClassType;                                                                                    \
    };

#define IE_SUB_CLASS_TYPE_DECLARE_WITH_NS(classBase, subClass, SubClassNS)                                             \
    template <>                                                                                                        \
    struct classBase##_SubClassTypeTraits<IE_SUB_CLASS_TYPE_NAME(classBase, subClass)> {                               \
        typedef SubClassNS::subClass ClassType;                                                                        \
    };

#define IE_SUB_CLASS_TYPE_SETUP(classBase, subClass) _objType = IE_SUB_CLASS_TYPE_NAME(classBase, subClass)

/*************** macro used in user class **************/
#define IE_CALL_SUB_CLASS_FUNC(classBase, subClassType, obj, funcName, args...)                                        \
    static_cast<typename classBase##_SubClassTypeTraits<subClassType>::ClassType*>(obj)->funcName(args)

// if more than 16 sub class, sub class after 16th will use vtable
#define IE_CALL_VIRTUAL_FUNC(classBase, obj, funcName, args...)                                                        \
    switch (obj->getObjectType()) {                                                                                    \
    case 0:                                                                                                            \
        return obj->funcName(##args);                                                                                  \
    case 1:                                                                                                            \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 1, obj, funcName, args);                                              \
    case 2:                                                                                                            \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 2, obj, funcName, args);                                              \
    case 3:                                                                                                            \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 3, obj, funcName, args);                                              \
    case 4:                                                                                                            \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 4, obj, funcName, args);                                              \
    case 5:                                                                                                            \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 5, obj, funcName, args);                                              \
    case 6:                                                                                                            \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 6, obj, funcName, args);                                              \
    case 7:                                                                                                            \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 7, obj, funcName, args);                                              \
    case 8:                                                                                                            \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 8, obj, funcName, args);                                              \
    case 9:                                                                                                            \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 9, obj, funcName, args);                                              \
    case 10:                                                                                                           \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 10, obj, funcName, args);                                             \
    case 11:                                                                                                           \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 11, obj, funcName, args);                                             \
    case 12:                                                                                                           \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 12, obj, funcName, args);                                             \
    case 13:                                                                                                           \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 13, obj, funcName, args);                                             \
    case 14:                                                                                                           \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 14, obj, funcName, args);                                             \
    case 15:                                                                                                           \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 15, obj, funcName, args);                                             \
    case 16:                                                                                                           \
        return IE_CALL_SUB_CLASS_FUNC(classBase, 16, obj, funcName, args);                                             \
    default:                                                                                                           \
        return obj->funcName(##args);                                                                                  \
    }
}} // namespace indexlib::util
