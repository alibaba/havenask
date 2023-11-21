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

#include "autil/NoCopyable.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/index/operation_log/OperationFieldInfo.h"

namespace indexlib::index {
class PrimaryKeyIndexReader;
} // namespace indexlib::index

namespace indexlib::index {
class OperationRedoHint;
class OperationLogProcessor;

class OperationBase : public autil::NoCopyable
{
public:
    enum SerializedOperationType { INVALID_SERIALIZE_OP = 0, REMOVE_OP = 1, UPDATE_FIELD_OP = 2, SUB_DOC_OP = 3 };

public:
    OperationBase(const indexlibv2::framework::Locator::DocInfo& docInfo) : _docInfo(docInfo) {}
    virtual ~OperationBase() {}

public:
    virtual bool Load(autil::mem_pool::Pool* pool, char*& cursor) = 0;
    virtual OperationBase* Clone(autil::mem_pool::Pool* pool) = 0;
    virtual SerializedOperationType GetSerializedType() const { return INVALID_SERIALIZE_OP; }
    virtual DocOperateType GetDocOperateType() const { return UNKNOWN_OP; }
    const indexlibv2::framework::Locator::DocInfo& GetDocInfo() const { return _docInfo; }
    virtual size_t GetMemoryUse() const = 0;
    virtual segmentid_t GetSegmentId() const = 0;
    virtual size_t GetSerializeSize() const = 0;
    virtual size_t Serialize(char* buffer, size_t bufferLen) const = 0;
    virtual const char* GetPkHashPointer() const = 0;
    //[docIdRange.first, docIdRange.second)
    virtual bool Process(const PrimaryKeyIndexReader* pkReader, OperationLogProcessor* processor,
                         const std::vector<std::pair<docid_t, docid_t>>& docidRange) const = 0;
    void SetOperationFieldInfo(const std::shared_ptr<OperationFieldInfo>& operationFieldInfo)
    {
        _operationFieldInfo = operationFieldInfo;
    }

protected:
    std::shared_ptr<OperationFieldInfo> _operationFieldInfo;
    indexlibv2::framework::Locator::DocInfo _docInfo;
};

} // namespace indexlib::index
