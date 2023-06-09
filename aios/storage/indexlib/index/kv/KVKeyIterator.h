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

#include "indexlib/index/kv/IKVIterator.h"

namespace indexlibv2::index {
class HashTableFileIterator;
class ValueUnpacker;

class KVKeyIterator final : public IKVIterator
{
public:
    KVKeyIterator(std::unique_ptr<HashTableFileIterator> hashIter, ValueUnpacker* valueUnpacker, bool isVarLen);
    ~KVKeyIterator();

public:
    bool HasNext() const override;
    Status Next(autil::mem_pool::Pool* pool, Record& record) override;
    Status Seek(offset_t offset) override;
    void Reset() override;
    offset_t GetOffset() const override;
    const IKVIterator* GetKeyIterator() const override;

    void SortByKey();
    void SortByValue();

private:
    std::unique_ptr<HashTableFileIterator> _hashIter;
    ValueUnpacker* _valueUnpacker = nullptr; // not owned
    bool _isVarLen;
};

} // namespace indexlibv2::index
