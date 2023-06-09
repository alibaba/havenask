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

#include "indexlib/index/common/AtomicValue.h"

namespace indexlib::index {

class MultiValue
{
public:
    MultiValue();
    ~MultiValue();

public:
    const AtomicValueVector& GetAtomicValues() const { return _atomicValues; }

    AtomicValue* GetAtomicValue(size_t index) const
    {
        assert(index < _atomicValues.size());
        return _atomicValues[index];
    }

    size_t GetAtomicValueSize() const { return _atomicValues.size(); }

    size_t GetTotalSize() const { return _size; }

    void AddAtomicValue(AtomicValue* atomicValue)
    {
        assert(atomicValue);
        _size += atomicValue->GetSize();
        _atomicValues.push_back(atomicValue);
    }

protected:
    AtomicValueVector _atomicValues;
    uint32_t _size;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
