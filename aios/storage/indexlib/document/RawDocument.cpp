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
#include "indexlib/document/RawDocument.h"

#include <utility>

#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/DocumentMemPoolFactory.h"
#include "indexlib/document/RawDocFieldIterator.h"
#include "indexlib/document/RawDocumentDefine.h"

namespace indexlibv2::document {

RawDocument::RawDocument()
{
    _recyclePool = indexlib::util::Singleton<DocumentMemPoolFactory>::GetInstance()->GetPool();
}

RawDocument::RawDocument(const RawDocument& other)
{
    _recyclePool = indexlib::util::Singleton<DocumentMemPoolFactory>::GetInstance()->GetPool();
}

RawDocument::~RawDocument()
{
    if (_recyclePool) {
        indexlib::util::Singleton<DocumentMemPoolFactory>::GetInstance()->Recycle(_recyclePool);
    }
}

RawDocument& RawDocument::operator=(const RawDocument& other)
{
    if (this != &other) {
        if (_recyclePool) {
            _recyclePool->reset();
        }
    }
    return *this;
}

RawDocument::RawDocument(RawDocument&& other)
    : _recyclePool(std::exchange(other._recyclePool, nullptr))
    , _ignoreEmptyField(other._ignoreEmptyField)
{
}

void RawDocument::setField(const char* fieldName, size_t nameLen, const char* fieldValue, size_t valueLen)
{
    autil::StringView name(fieldName, nameLen);
    autil::StringView value(fieldValue, valueLen);
    setField(name, value);
}

void RawDocument::setField(const std::string& fieldName, const std::string& fieldValue)
{
    setField(fieldName.data(), fieldName.length(), fieldValue.data(), fieldValue.length());
}

std::string RawDocument::getField(const std::string& fieldName) const
{
    const auto& constStr = getField(autil::StringView(fieldName));
    if (constStr.data()) {
        return std::string(constStr.data(), constStr.size());
    }
    static const autil::StringView EMPTY_STRING;
    return std::string(EMPTY_STRING.data(), EMPTY_STRING.size());
}

bool RawDocument::exist(const std::string& fieldName) const { return exist(autil::StringView(fieldName)); }

void RawDocument::eraseField(const std::string& fieldName) { eraseField(autil::StringView(fieldName)); }

bool RawDocument::NeedTrace() const { return !GetTraceId().empty(); }

std::string RawDocument::GetTraceId() const { return getField(BUILTIN_KEY_TRACE_ID); }

autil::StringView RawDocument::GetEvaluatorState() const { return _evaluatorState; }
void RawDocument::SetEvaluatorState(const autil::StringView& state) { _evaluatorState = state; }
autil::mem_pool::Pool* RawDocument::getPool() const { return _recyclePool; }

const std::string& RawDocument::getDocSource() const { return GetTag(DOCUMENT_SOURCE_TAG_KEY); }
void RawDocument::setDocSource(const std::string& src) { AddTag(DOCUMENT_SOURCE_TAG_KEY, src); }

DocOperateType RawDocument::getDocOperateType(const autil::StringView& cmdString)
{
    if (cmdString.empty()) {
        return ADD_DOC;
    } else if (cmdString == CMD_ADD) {
        return ADD_DOC;
    } else if (cmdString == CMD_DELETE) {
        return DELETE_DOC;
    } else if (cmdString == CMD_DELETE_SUB) {
        return DELETE_SUB_DOC;
    } else if (cmdString == CMD_UPDATE_FIELD) {
        return UPDATE_FIELD;
    } else if (cmdString == CMD_SKIP) {
        return SKIP_DOC;
    } else if (cmdString == CMD_CHECKPOINT) {
        return CHECKPOINT_DOC;
    } else if (cmdString == CMD_ALTER) {
        return ALTER_DOC;
    } else if (cmdString == CMD_BULKLOAD) {
        return BULKLOAD_DOC;
    }
    return UNKNOWN_OP;
}

const std::string& RawDocument::getCmdString(DocOperateType type)
{
    switch (type) {
    case ADD_DOC:
        return CMD_ADD;
    case UPDATE_FIELD:
        return CMD_UPDATE_FIELD;
    case DELETE_DOC:
        return CMD_DELETE;
    case DELETE_SUB_DOC:
        return CMD_DELETE_SUB;
    case SKIP_DOC:
        return CMD_SKIP;
    case CHECKPOINT_DOC:
        return CMD_CHECKPOINT;
    case ALTER_DOC:
        return CMD_ALTER;
    case BULKLOAD_DOC:
        return CMD_BULKLOAD;
    default:
        return CMD_UNKNOWN;
    }
    return CMD_UNKNOWN;
}

void RawDocument::setIgnoreEmptyField(bool ignoreEmptyField) { _ignoreEmptyField = ignoreEmptyField; }
bool RawDocument::ignoreEmptyField() const { return _ignoreEmptyField; }

bool RawDocument::IsUserDoc()
{
    auto opType = getDocOperateType();
    return opType == UNKNOWN_OP || opType == ADD_DOC || opType == DELETE_DOC || opType == UPDATE_FIELD ||
           opType == SKIP_DOC || opType == DELETE_SUB_DOC;
}

RawDocument::Snapshot* RawDocument::GetSnapshot() const
{
    auto snapshot = new Snapshot();
    for (auto iter = std::unique_ptr<RawDocFieldIterator>(CreateIterator()); iter->IsValid(); iter->MoveToNext()) {
        snapshot->emplace(iter->GetFieldName(), iter->GetFieldValue());
    }
    return snapshot;
}

} // namespace indexlibv2::document
