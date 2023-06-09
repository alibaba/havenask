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
#include <type_traits>
#include "autil/DataBuffer.h"
#include "autil/MultiValueType.h"
#include "autil/Hyperloglog.h"

namespace matchdoc {

struct SupportSerializeType {};
struct UnSupportSerializeType {};

template<typename T>
struct SupportSerializeTrait {
    typedef SupportSerializeType type;
};

struct SupportCloneType {};
struct UnSupportCloneType {};
template <typename T>
struct SupportCloneTrait {
    typedef SupportCloneType type;
};

typedef std::string VariableType;

struct SimpleType {};
struct SimpleVectorType {};
struct TwoLevelVectorType {};
struct ToStringType{};

template<typename T, bool =
         std::is_arithmetic<T>::value ||
         std::is_enum<T>::value>
struct ToStringTypeTraits {
    typedef ToStringType Type;
};

template<typename T>
struct ToStringTypeTraits<T, true> {
    typedef SimpleType Type;
};

template<typename T>
struct ConstructTypeTraits {
    static constexpr bool NeedConstruct()
    {
        return true;
    }
};

template<typename T>
struct DestructTypeTraits {
    static bool NeedDestruct()
    {
        return true;
    }
};

template<typename T>
struct CanMountTypeTraits {
    static bool canMount()
    {
        return false;
    }
};

template<typename T>
struct CanDataMountTypeTraits {
    static bool canDataMount()
    {
        return false;
    }
};

}

#define NOT_SUPPORT_SERIZLIE_TYPE(T)            \
    namespace matchdoc {                        \
    template<>                                  \
    struct SupportSerializeTrait<T> {           \
        typedef UnSupportSerializeType type;    \
    };                                          \
    }

#define NOT_SUPPORT_CLONE_TYPE(T)               \
    namespace matchdoc {                        \
    template <>                                 \
    struct SupportCloneTrait<T> {               \
        typedef UnSupportCloneType type;        \
    };                                          \
    }

#define DECLARE_TO_STRING_TYPE(T)               \
    namespace matchdoc {                                 \
    template<> struct ToStringTypeTraits<T, false> {     \
        typedef SimpleType Type;                         \
    };                                                   \
    }

DECLARE_TO_STRING_TYPE(autil::MultiChar);
DECLARE_TO_STRING_TYPE(autil::MultiInt8);
DECLARE_TO_STRING_TYPE(autil::MultiUInt8);
DECLARE_TO_STRING_TYPE(autil::MultiInt16);
DECLARE_TO_STRING_TYPE(autil::MultiUInt16);
DECLARE_TO_STRING_TYPE(autil::MultiInt32);
DECLARE_TO_STRING_TYPE(autil::MultiUInt32);
DECLARE_TO_STRING_TYPE(autil::MultiInt64);
DECLARE_TO_STRING_TYPE(autil::MultiUInt64);
DECLARE_TO_STRING_TYPE(autil::MultiFloat);
DECLARE_TO_STRING_TYPE(autil::MultiDouble);
DECLARE_TO_STRING_TYPE(autil::MultiString);
DECLARE_TO_STRING_TYPE(std::string);
DECLARE_TO_STRING_TYPE(autil::uint128_t);
DECLARE_TO_STRING_TYPE(autil::HllCtx);

#define DECLARE_TO_STRING_VECTOR_TYPE(T)                                \
    namespace matchdoc {                                                \
    template<> struct ToStringTypeTraits<std::vector<T>, false> {       \
        typedef SimpleVectorType Type;                                  \
    };                                                                  \
    }

DECLARE_TO_STRING_VECTOR_TYPE(uint8_t);
DECLARE_TO_STRING_VECTOR_TYPE(uint16_t);
DECLARE_TO_STRING_VECTOR_TYPE(uint32_t);
DECLARE_TO_STRING_VECTOR_TYPE(uint64_t);
DECLARE_TO_STRING_VECTOR_TYPE(int8_t);
DECLARE_TO_STRING_VECTOR_TYPE(int16_t);
DECLARE_TO_STRING_VECTOR_TYPE(int32_t);
DECLARE_TO_STRING_VECTOR_TYPE(int64_t);
DECLARE_TO_STRING_VECTOR_TYPE(double);
DECLARE_TO_STRING_VECTOR_TYPE(float);
DECLARE_TO_STRING_VECTOR_TYPE(bool);
DECLARE_TO_STRING_VECTOR_TYPE(std::string);
DECLARE_TO_STRING_VECTOR_TYPE(long long);
DECLARE_TO_STRING_VECTOR_TYPE(unsigned long long);

#define DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(T)                                      \
    namespace matchdoc {                                                             \
    template<> struct ToStringTypeTraits<std::vector<std::vector<T>>, false> {       \
        typedef TwoLevelVectorType Type;                                             \
    };                                                                               \
    }

DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(uint8_t);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(uint16_t);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(uint32_t);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(uint64_t);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(int8_t);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(int16_t);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(int32_t);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(int64_t);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(double);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(float);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(bool);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(std::string);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(long long);
DECLARE_TWO_LEVEL_STRING_VECTOR_TYPE(unsigned long long);


#define DECLARE_NO_NEED_CONSTRUCT_TYPE(T)        \
    namespace matchdoc {                         \
    template<>                                   \
    struct ConstructTypeTraits<T> {              \
        static constexpr bool NeedConstruct()    \
        {                                        \
            return false;                        \
        }                                        \
    };                                           \
    }

DECLARE_NO_NEED_CONSTRUCT_TYPE(uint8_t);
DECLARE_NO_NEED_CONSTRUCT_TYPE(uint16_t);
DECLARE_NO_NEED_CONSTRUCT_TYPE(uint32_t);
DECLARE_NO_NEED_CONSTRUCT_TYPE(uint64_t);
DECLARE_NO_NEED_CONSTRUCT_TYPE(int8_t);
DECLARE_NO_NEED_CONSTRUCT_TYPE(int16_t);
DECLARE_NO_NEED_CONSTRUCT_TYPE(int32_t);
DECLARE_NO_NEED_CONSTRUCT_TYPE(int64_t);
DECLARE_NO_NEED_CONSTRUCT_TYPE(double);
DECLARE_NO_NEED_CONSTRUCT_TYPE(float);
DECLARE_NO_NEED_CONSTRUCT_TYPE(bool);


#define DECLARE_NO_NEED_DESTRUCT_TYPE(T)        \
    namespace matchdoc {                        \
    template<>                                  \
    struct DestructTypeTraits<T> {              \
        static bool NeedDestruct()              \
        {                                       \
            return false;                       \
        }                                       \
    };                                          \
    }

DECLARE_NO_NEED_DESTRUCT_TYPE(uint8_t);
DECLARE_NO_NEED_DESTRUCT_TYPE(uint16_t);
DECLARE_NO_NEED_DESTRUCT_TYPE(uint32_t);
DECLARE_NO_NEED_DESTRUCT_TYPE(uint64_t);
DECLARE_NO_NEED_DESTRUCT_TYPE(int8_t);
DECLARE_NO_NEED_DESTRUCT_TYPE(int16_t);
DECLARE_NO_NEED_DESTRUCT_TYPE(int32_t);
DECLARE_NO_NEED_DESTRUCT_TYPE(int64_t);
DECLARE_NO_NEED_DESTRUCT_TYPE(double);
DECLARE_NO_NEED_DESTRUCT_TYPE(float);
DECLARE_NO_NEED_DESTRUCT_TYPE(bool);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiChar);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiInt8);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiUInt8);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiInt16);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiUInt16);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiInt32);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiUInt32);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiInt64);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiUInt64);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiFloat);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiDouble);
DECLARE_NO_NEED_DESTRUCT_TYPE(autil::MultiString);


#define DECLARE_CAN_MOUNT_TYPE(T)               \
    namespace matchdoc {                        \
    template<>                                  \
    struct CanMountTypeTraits<T> {              \
        static bool canMount()                  \
        {                                       \
            return true;                        \
        }                                       \
    };                                          \
    }

DECLARE_CAN_MOUNT_TYPE(uint8_t);
DECLARE_CAN_MOUNT_TYPE(uint16_t);
DECLARE_CAN_MOUNT_TYPE(uint32_t);
DECLARE_CAN_MOUNT_TYPE(uint64_t);
DECLARE_CAN_MOUNT_TYPE(int8_t);
DECLARE_CAN_MOUNT_TYPE(int16_t);
DECLARE_CAN_MOUNT_TYPE(int32_t);
DECLARE_CAN_MOUNT_TYPE(int64_t);
DECLARE_CAN_MOUNT_TYPE(double);
DECLARE_CAN_MOUNT_TYPE(float);
DECLARE_CAN_MOUNT_TYPE(bool);

DECLARE_CAN_MOUNT_TYPE(autil::MultiChar);
DECLARE_CAN_MOUNT_TYPE(autil::MultiInt8);
DECLARE_CAN_MOUNT_TYPE(autil::MultiUInt8);
DECLARE_CAN_MOUNT_TYPE(autil::MultiInt16);
DECLARE_CAN_MOUNT_TYPE(autil::MultiUInt16);
DECLARE_CAN_MOUNT_TYPE(autil::MultiInt32);
DECLARE_CAN_MOUNT_TYPE(autil::MultiUInt32);
DECLARE_CAN_MOUNT_TYPE(autil::MultiInt64);
DECLARE_CAN_MOUNT_TYPE(autil::MultiUInt64);
DECLARE_CAN_MOUNT_TYPE(autil::MultiFloat);
DECLARE_CAN_MOUNT_TYPE(autil::MultiDouble);
DECLARE_CAN_MOUNT_TYPE(autil::MultiString);


#define DECLARE_CAN_DATAMOUNT_TYPE(T)           \
    namespace matchdoc {                        \
    template<>                                  \
    struct CanDataMountTypeTraits<T> {          \
        static bool canDataMount()              \
        {                                       \
            return true;                        \
        }                                       \
    };                                          \
    }

DECLARE_CAN_DATAMOUNT_TYPE(uint8_t);
DECLARE_CAN_DATAMOUNT_TYPE(uint16_t);
DECLARE_CAN_DATAMOUNT_TYPE(uint32_t);
DECLARE_CAN_DATAMOUNT_TYPE(uint64_t);
DECLARE_CAN_DATAMOUNT_TYPE(int8_t);
DECLARE_CAN_DATAMOUNT_TYPE(int16_t);
DECLARE_CAN_DATAMOUNT_TYPE(int32_t);
DECLARE_CAN_DATAMOUNT_TYPE(int64_t);
DECLARE_CAN_DATAMOUNT_TYPE(double);
DECLARE_CAN_DATAMOUNT_TYPE(float);
DECLARE_CAN_DATAMOUNT_TYPE(bool);
