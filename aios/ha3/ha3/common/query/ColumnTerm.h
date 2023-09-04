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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/MultiValueType.h"
#include "matchdoc/ValueType.h"

namespace autil {
class DataBuffer;
} // namespace autil

namespace isearch {
namespace common {

class ColumnTerm {
public:
    ColumnTerm(const std::string &indexName = std::string(), bool cache = true);
    virtual ~ColumnTerm() = default;

public:
    bool getEnableCache() const {
        return _enableCache;
    }

    void setEnableCache(bool enableCache) {
        _enableCache = enableCache;
    }

    const std::string &getIndexName() const {
        return _indexName;
    }

    std::vector<size_t> &getOffsets() {
        return _offsets;
    }

    const std::vector<size_t> &getOffsets() const {
        return _offsets;
    }

    bool isMultiValueTerm() const {
        return !_offsets.empty();
    }

    std::string offsetsToString() const;

public:
    static void serialize(const ColumnTerm *p, autil::DataBuffer &dataBuffer);
    static ColumnTerm *deserialize(autil::DataBuffer &dataBuffer);
    static ColumnTerm *createColumnTerm(matchdoc::BuiltinType type);

public:
    virtual bool operator==(const ColumnTerm &term) const = 0;
    virtual ColumnTerm *clone() const = 0;
    virtual std::string toString() const = 0;
    virtual matchdoc::BuiltinType getValueType() const = 0;

protected:
    virtual void doSave(autil::DataBuffer &dataBuffer) const = 0;
    virtual void doLoad(autil::DataBuffer &dataBuffer) = 0;

private:
    void save(autil::DataBuffer &dataBuffer) const;
    void load(autil::DataBuffer &dataBuffer);

protected:
    bool _enableCache;
    std::string _indexName;
    std::vector<size_t> _offsets;

protected:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ColumnTerm> ColumnTermPtr;

template <class T>
class ColumnTermTyped : public ColumnTerm {
public:
    using ColumnTerm::ColumnTerm;

public:
    std::vector<T> &getValues();
    const std::vector<T> &getValues() const;

public:
    bool operator==(const ColumnTerm &term) const override;
    ColumnTerm *clone() const override;
    std::string toString() const override;
    matchdoc::BuiltinType getValueType() const override;

protected:
    void doSave(autil::DataBuffer &dataBuffer) const override;
    void doLoad(autil::DataBuffer &dataBuffer) override;

private:
    std::vector<T> _values;
};

extern template class ColumnTermTyped<uint64_t>;
extern template class ColumnTermTyped<int64_t>;
extern template class ColumnTermTyped<uint32_t>;
extern template class ColumnTermTyped<int32_t>;
extern template class ColumnTermTyped<uint16_t>;
extern template class ColumnTermTyped<int16_t>;
extern template class ColumnTermTyped<uint8_t>;
extern template class ColumnTermTyped<int8_t>;
extern template class ColumnTermTyped<bool>;
extern template class ColumnTermTyped<float>;
extern template class ColumnTermTyped<double>;
extern template class ColumnTermTyped<autil::MultiChar>;

} // namespace common
} // namespace isearch
