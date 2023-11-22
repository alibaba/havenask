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

#include "autil/Hyperloglog.h"
#include "matchdoc/VectorStorage.h"

namespace table {

class HllCtxColumnData final {
public:
    HllCtxColumnData() = default;
    explicit HllCtxColumnData(const std::shared_ptr<autil::mem_pool::Pool> &pool)
        : HllCtxColumnData(std::make_shared<matchdoc::VectorStorage>(
                               pool, sizeof(autil::HllCtx) + autil::Hyperloglog::HLL_REGISTERS * sizeof(uint8_t)),
                           0) {}
    HllCtxColumnData(std::shared_ptr<matchdoc::VectorStorage> data, int64_t offset)
        : _data(std::move(data)), _offset(offset) {}
    ~HllCtxColumnData() = default;

public:
    uint32_t capacity() const { return _data->getSize(); }
    uint32_t usedBytes(uint32_t rowCount) const { return _data->getDocSize() * rowCount; }
    uint32_t allocatedBytes() const { return _data->getDocSize() * capacity(); }

    void resize(size_t capacity) { _data->growToSize(capacity); }

    autil::mem_pool::Pool *getPool() const { return _data->getPool(); }

    const std::shared_ptr<matchdoc::VectorStorage> &getData() const { return _data; }

    int64_t getOffset() const { return _offset; }

    bool merge(HllCtxColumnData &other) {
        _data->append(*other._data);
        return true;
    }

    autil::HllCtx get(size_t index) const {
        auto data = _data->get(index);
        autil::HllCtx *src = reinterpret_cast<autil::HllCtx *>(data);
        autil::HllCtx dst;
        dst._encoding = src->_encoding;
        assert(!dst._encoding);
        dst._eleNum = src->_eleNum;
        dst._registers = (uint8_t *)(reinterpret_cast<char *>(data) + sizeof(autil::HllCtx));
        return dst;
    }

    void set(size_t index, const autil::HllCtx &value) {
        auto data = _data->get(index);
        autil::HllCtx *dst = reinterpret_cast<autil::HllCtx *>(data);
        assert(!value._encoding);
        dst->_encoding = value._encoding;
        dst->_eleNum = value._eleNum;
        dst->_registers = (uint8_t *)(reinterpret_cast<char *>(data) + sizeof(autil::HllCtx));
        memcpy(dst->_registers, value._registers, autil::Hyperloglog::HLL_REGISTERS * sizeof(uint8_t));
    }

private:
    std::shared_ptr<matchdoc::VectorStorage> _data;
    int64_t _offset = 0;
};

} // namespace table
