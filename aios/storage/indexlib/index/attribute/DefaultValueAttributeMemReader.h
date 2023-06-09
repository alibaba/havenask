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

#include "autil/Log.h"
#include "autil/MultiValueType.h"
#include "autil/NoCopyable.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::config {
class AttributeConfig;
}

namespace indexlibv2::index {

class DefaultValueAttributeMemReader : private NoCopyable
{
public:
    DefaultValueAttributeMemReader() = default;
    ~DefaultValueAttributeMemReader() = default;

public:
    Status Open(const std::shared_ptr<config::AttributeConfig>& attrConfig);

    template <typename T>
    bool ReadSingleValue(docid_t docId, T& attrValue, bool& isNull)
    {
        assert(sizeof(T) == _defaultValue.size());
        isNull = false;
        attrValue = *((T*)(_defaultValue.data()));
        return true;
    }

    template <typename T>
    bool ReadMultiValue(docid_t docId, autil::MultiValueType<T>& attrValue, bool& isNull)
    {
        isNull = false;
        if (_fixedValueCount == -1) {
            attrValue.init((const void*)_defaultValue.data());
        } else {
            attrValue = autil::MultiValueType<T>(reinterpret_cast<const char*>(_defaultValue.data()), _fixedValueCount);
        }
        return true;
    }

private:
    autil::mem_pool::Pool _pool;
    autil::StringView _defaultValue;
    int32_t _fixedValueCount = -1;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
