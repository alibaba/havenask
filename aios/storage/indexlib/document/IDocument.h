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
#include "autil/StringView.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/Locator.h"

namespace autil {
class DataBuffer;
}

namespace indexlibv2::document {

class IDocument
{
public:
    IDocument() {}
    virtual ~IDocument() {}

public:
    virtual const framework::Locator& GetLocatorV2() const = 0;
    virtual framework::Locator::DocInfo GetDocInfo() const = 0;
    virtual uint32_t GetTTL() const = 0;
    virtual docid_t GetDocId() const = 0;
    virtual DocOperateType GetDocOperateType() const = 0;
    virtual bool HasFormatError() const = 0;
    virtual autil::StringView GetTraceId() const = 0;
    virtual int64_t GetIngestionTimestamp() const = 0;
    virtual autil::StringView GetSource() const = 0;
    virtual schemaid_t GetSchemaId() const = 0;

public:
    virtual void SetLocator(const framework::Locator& locator) = 0;
    virtual void SetDocInfo(const framework::Locator::DocInfo& docInfo) = 0;
    virtual void SetDocOperateType(DocOperateType type) = 0;
    virtual void SetTrace(bool trace) = 0;

public:
    virtual size_t EstimateMemory() const = 0;
    virtual void serialize(autil::DataBuffer& dataBuffer) const = 0;
    virtual void deserialize(autil::DataBuffer& dataBuffer) = 0;
};

} // namespace indexlibv2::document
