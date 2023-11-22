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
#include "indexlib/document/document_parser.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class LineDataDocumentParser : public DocumentParser
{
public:
    LineDataDocumentParser(const config::IndexPartitionSchemaPtr& schema);
    ~LineDataDocumentParser();

public:
    bool DoInit() override;
    DocumentPtr Parse(const IndexlibExtendDocumentPtr& extendDoc) override;
    DocumentPtr Parse(const autil::StringView& serializedData) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LineDataDocumentParser);
}} // namespace indexlib::document
