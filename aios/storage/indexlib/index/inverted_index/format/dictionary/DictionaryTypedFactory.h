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

#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/util/Singleton.h"
namespace indexlib::index {

template <typename ClassBase, template <typename T> class ClassTyped>
class DictionaryTypedFactory : public util::Singleton<DictionaryTypedFactory<ClassBase, ClassTyped>>
{
protected:
    DictionaryTypedFactory() {}
    friend class util::LazyInstantiation;

public:
    ~DictionaryTypedFactory() {}

    ClassBase* Create(InvertedIndexType indexType)
    {
        switch (indexType) {
        case it_number_int32:
        case it_number_uint32:
            return new ClassTyped<uint32_t>();
        case it_number_int64:
        case it_number_uint64:
            return new ClassTyped<uint64_t>();
        case it_number_int8:
        case it_number_uint8:
            return new ClassTyped<uint8_t>();
        case it_number_int16:
        case it_number_uint16:
            return new ClassTyped<uint16_t>();
        case it_unknown:
        case it_number:
            assert(false);
            return nullptr;
        default:
            return new ClassTyped<dictkey_t>();
            ;
        }
        return nullptr;
    }

    ClassBase* Create(InvertedIndexType indexType, autil::mem_pool::PoolBase* pool)
    {
        switch (indexType) {
        case it_number_int32:
        case it_number_uint32:
            return new ClassTyped<uint32_t>(pool);
        case it_number_int64:
        case it_number_uint64:
            return new ClassTyped<uint64_t>(pool);
        case it_number_int8:
        case it_number_uint8:
            return new ClassTyped<uint8_t>(pool);
        case it_number_int16:
        case it_number_uint16:
            return new ClassTyped<uint16_t>(pool);
        case it_unknown:
        case it_number:
            assert(false);
            return nullptr;
        default:
            return new ClassTyped<dictkey_t>(pool);
        }
        return nullptr;
    }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
