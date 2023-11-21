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

#include "indexlib/common_define.h"
#include "indexlib/document/document.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace document {

/* document parser */
class DocumentParser
{
public:
    DocumentParser(const config::IndexPartitionSchemaPtr& schema) : mSchema(schema) {}

    virtual ~DocumentParser() {}

public:
    virtual bool DoInit() = 0;

    virtual DocumentPtr ParseRawDoc(const RawDocumentPtr& rawDoc)
    {
        IndexlibExtendDocumentPtr extendDoc(new IndexlibExtendDocument);
        extendDoc->SetRawDocument(rawDoc);
        return Parse(extendDoc);
    }

    virtual DocumentPtr Parse(const IndexlibExtendDocumentPtr& extendDoc) = 0;
    // dataStr: serialized data which from call document serialize interface
    virtual DocumentPtr Parse(const autil::StringView& serializedData) = 0;

    virtual DocumentPtr TEST_ParseMultiMessage(const autil::StringView& serializedData)
    {
        assert(false);
        return DocumentPtr();
    }

public:
    bool Init(const DocumentInitParamPtr& initParam)
    {
        mInitParam = initParam;
        return DoInit();
    }

    const DocumentInitParamPtr& GetInitParam() const { return mInitParam; }

protected:
    config::IndexPartitionSchemaPtr mSchema;
    DocumentInitParamPtr mInitParam;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentParser);
}} // namespace indexlib::document
