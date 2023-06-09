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

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"

namespace indexlib { namespace document {

class SearchSummaryDocument
{
public:
    SearchSummaryDocument(autil::mem_pool::PoolBase* pool, size_t size);
    ~SearchSummaryDocument();

public:
    // for deserialize summary hit
    void UpdateFieldCount(size_t size);
    char* AllocateFromBuffer(size_t size);
    autil::mem_pool::PoolBase* getPool();

public:
    bool SetFieldValue(int32_t summaryFieldId, const char* fieldValue, size_t size, bool needCopy = true);
    const autil::StringView* GetFieldValue(int32_t summaryFieldId) const;

    void ClearFieldValue(int32_t summaryFieldId);

    size_t GetFieldCount() const { return _fieldValues.size(); }

    char* GetBuffer(size_t size);
    void addConstString(size_t count);
    const std::vector<autil::StringView*>& GetFields() const { return _fieldValues; }

private:
    bool _ownPool;
    autil::mem_pool::PoolBase* _pool;
    std::vector<autil::StringView*> _fieldValues;
    char* _buffer;
    size_t _bufferUsed;
    std::vector<autil::StringView*> _valPool;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::document
