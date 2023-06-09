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

#include "autil/LongHashValue.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/util/Singleton.h"

namespace indexlib { namespace util {

struct EmptyParam {
};

template <typename ClassBase, template <typename T> class ClassTyped, typename Param1 = EmptyParam,
          typename Param2 = EmptyParam>
class ClassTypedFactory : public util::Singleton<ClassTypedFactory<ClassBase, ClassTyped, Param1, Param2>>
{
protected:
    ClassTypedFactory() {}
    friend class util::LazyInstantiation;

public:
    ~ClassTypedFactory() {}

public:
    ClassBase* Create(FieldType fieldType)
    {
        switch (fieldType) {
        case ft_int8:
            return new ClassTyped<int8_t>();
        case ft_uint8:
            return new ClassTyped<uint8_t>();
        case ft_int16:
            return new ClassTyped<int16_t>();
        case ft_uint16:
            return new ClassTyped<uint16_t>();
        case ft_int32:
            return new ClassTyped<int32_t>();
        case ft_uint32:
            return new ClassTyped<uint32_t>();
        case ft_int64:
            return new ClassTyped<int64_t>();
        case ft_hash_64:
        case ft_uint64:
            return new ClassTyped<uint64_t>();
        case ft_float:
            return new ClassTyped<float>();
        case ft_double:
            return new ClassTyped<double>();
        case ft_fp8:
            return new ClassTyped<int8_t>();
        case ft_fp16:
            return new ClassTyped<int16_t>();
        case ft_time:
        case ft_date:
            return new ClassTyped<uint32_t>();
        case ft_timestamp:
            return new ClassTyped<uint64_t>();
        case ft_hash_128:
            return new ClassTyped<autil::uint128_t>();
        default:
            assert(false);
        }
        return NULL;
    }

    ClassBase* Create(FieldType fieldType, Param1 param1)
    {
        switch (fieldType) {
        case ft_int8:
            return new ClassTyped<int8_t>(param1);
        case ft_uint8:
            return new ClassTyped<uint8_t>(param1);
        case ft_int16:
            return new ClassTyped<int16_t>(param1);
        case ft_uint16:
            return new ClassTyped<uint16_t>(param1);
        case ft_int32:
            return new ClassTyped<int32_t>(param1);
        case ft_uint32:
            return new ClassTyped<uint32_t>(param1);
        case ft_int64:
            return new ClassTyped<int64_t>(param1);
        case ft_hash_64:
        case ft_uint64:
            return new ClassTyped<uint64_t>(param1);
        case ft_float:
            return new ClassTyped<float>(param1);
        case ft_double:
            return new ClassTyped<double>(param1);
        case ft_fp8:
            return new ClassTyped<int8_t>(param1);
        case ft_fp16:
            return new ClassTyped<int16_t>(param1);
        case ft_date:
        case ft_time:
            return new ClassTyped<uint32_t>(param1);
        case ft_timestamp:
            return new ClassTyped<uint64_t>(param1);
        case ft_hash_128:
            return new ClassTyped<autil::uint128_t>(param1);
        default:
            assert(false);
        }
        return NULL;
    }

    ClassBase* Create(FieldType fieldType, Param1 param1, Param2 param2)
    {
        switch (fieldType) {
        case ft_int8:
            return new ClassTyped<int8_t>(param1, param2);
        case ft_uint8:
            return new ClassTyped<uint8_t>(param1, param2);
        case ft_int16:
            return new ClassTyped<int16_t>(param1, param2);
        case ft_uint16:
            return new ClassTyped<uint16_t>(param1, param2);
        case ft_int32:
            return new ClassTyped<int32_t>(param1, param2);
        case ft_uint32:
            return new ClassTyped<uint32_t>(param1, param2);
        case ft_int64:
            return new ClassTyped<int64_t>(param1, param2);
        case ft_hash_64:
        case ft_uint64:
            return new ClassTyped<uint64_t>(param1, param2);
        case ft_float:
            return new ClassTyped<float>(param1, param2);
        case ft_double:
            return new ClassTyped<double>(param1, param2);
        case ft_fp8:
            return new ClassTyped<int8_t>(param1, param2);
        case ft_fp16:
            return new ClassTyped<int16_t>(param1, param2);
        case ft_date:
        case ft_time:
            return new ClassTyped<uint32_t>(param1, param2);
        case ft_timestamp:
            return new ClassTyped<uint64_t>(param1, param2);
        case ft_hash_128:
            return new ClassTyped<autil::uint128_t>(param1, param2);
        default:
            assert(false);
        }
        return NULL;
    }

    ClassBase* FuncCreate(FieldType fieldType, Param1 param1, Param2 param2)
    {
        switch (fieldType) {
        case ft_int8:
            return ClassTyped<int8_t>::Create(param1, param2);
        case ft_uint8:
            return ClassTyped<uint8_t>::Create(param1, param2);
        case ft_int16:
            return ClassTyped<int16_t>::Create(param1, param2);
        case ft_uint16:
            return ClassTyped<uint16_t>::Create(param1, param2);
        case ft_int32:
            return ClassTyped<int32_t>::Create(param1, param2);
        case ft_uint32:
            return ClassTyped<uint32_t>::Create(param1, param2);
        case ft_int64:
            return ClassTyped<int64_t>::Create(param1, param2);
        case ft_hash_64:
        case ft_uint64:
            return ClassTyped<uint64_t>::Create(param1, param2);
        case ft_float:
            return ClassTyped<float>::Create(param1, param2);
        case ft_double:
            return ClassTyped<double>::Create(param1, param2);
        case ft_fp8:
            return ClassTyped<int8_t>::Create(param1, param2);
        case ft_fp16:
            return ClassTyped<int16_t>::Create(param1, param2);
        case ft_date:
        case ft_time:
            return ClassTyped<uint32_t>::Create(param1, param2);
        case ft_timestamp:
            return ClassTyped<uint64_t>::Create(param1, param2);
        case ft_hash_128:
            return ClassTyped<autil::uint128_t>::Create(param1, param2);
        default:
            assert(false);
        }
        return NULL;
    }
};
}} // namespace indexlib::util
