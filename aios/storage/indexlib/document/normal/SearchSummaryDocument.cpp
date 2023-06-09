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
#include "indexlib/document/normal/SearchSummaryDocument.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, SearchSummaryDocument);
static const size_t DEFAULT_SUMMARY_DOC_SIZE = 2048;

SearchSummaryDocument::SearchSummaryDocument(PoolBase* pool, size_t size)
    : _ownPool(pool == NULL)
    , _pool(pool ? pool : new Pool())
    , _buffer(NULL)
    , _bufferUsed(0)

{
    _fieldValues.resize(size, NULL);
    addConstString(size);
}

SearchSummaryDocument::~SearchSummaryDocument()
{
    if (_ownPool) {
        delete _pool;
        _pool = NULL;
    }
}

void SearchSummaryDocument::addConstString(size_t count)
{
    if (count == 0 || _valPool.size() > count) {
        return;
    }
    _valPool.resize(count, nullptr);
    char* arr = AllocateFromBuffer(count * sizeof(StringView));
    for (size_t i = 0; i < count; i++) {
        _valPool[i] = new (arr + sizeof(StringView) * i) StringView();
    }
}

char* SearchSummaryDocument::GetBuffer(size_t size) { return static_cast<char*>(_pool->allocate(size)); }

void SearchSummaryDocument::UpdateFieldCount(size_t size)
{
    if (_fieldValues.size() < size) {
        _fieldValues.resize(size, NULL);
    }
}

char* SearchSummaryDocument::AllocateFromBuffer(size_t size)
{
    if (_buffer == NULL) {
        _buffer = (char*)_pool->allocate(DEFAULT_SUMMARY_DOC_SIZE);
        if (_buffer == NULL) {
            return NULL;
        }
        _bufferUsed = 0;
    }

    if (size + _bufferUsed >= DEFAULT_SUMMARY_DOC_SIZE) {
        return (char*)_pool->allocate(size);
    } else {
        char* start = _buffer + _bufferUsed;
        _bufferUsed += size;
        return start;
    }
}

autil::mem_pool::PoolBase* SearchSummaryDocument::getPool() { return _pool; }

bool SearchSummaryDocument::SetFieldValue(int32_t summaryFieldId, const char* fieldValue, size_t size, bool needCopy)
{
    assert(summaryFieldId >= 0);
    if (summaryFieldId >= (int32_t)_fieldValues.size()) {
        _fieldValues.resize(summaryFieldId + 1, NULL);
    }
    const char* value = fieldValue;
    if (needCopy) {
        char* tmpValue = AllocateFromBuffer(size);
        if (NULL == tmpValue) {
            return false;
        }
        memcpy(tmpValue, fieldValue, size);
        value = tmpValue;
    }
    if (_valPool.size() == 0) {
        addConstString(16);
    }
    _fieldValues[summaryFieldId] = _valPool.back();
    *_fieldValues[summaryFieldId] = {value, size};
    _valPool.pop_back();
    return true;
}

const StringView* SearchSummaryDocument::GetFieldValue(int32_t summaryFieldId) const
{
    if (summaryFieldId < 0 || summaryFieldId >= (int32_t)_fieldValues.size()) {
        return NULL;
    }
    return _fieldValues[summaryFieldId];
}

void SearchSummaryDocument::ClearFieldValue(int32_t summaryFieldId)
{
    assert(summaryFieldId >= 0);
    if (summaryFieldId >= (int32_t)_fieldValues.size()) {
        _fieldValues.resize(summaryFieldId + 1, NULL);
    }
    _fieldValues[summaryFieldId] = NULL;
}
}} // namespace indexlib::document
