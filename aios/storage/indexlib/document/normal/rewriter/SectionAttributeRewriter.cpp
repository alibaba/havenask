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
#include "indexlib/document/normal/rewriter/SectionAttributeRewriter.h"

#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/rewriter/SectionAttributeAppender.h"

using namespace std;
using namespace indexlibv2::config;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, SectionAttributeRewriter);

SectionAttributeRewriter::SectionAttributeRewriter() {}

SectionAttributeRewriter::~SectionAttributeRewriter() {}

bool SectionAttributeRewriter::Init(const shared_ptr<ITabletSchema>& schema)
{
    auto mainAppender = make_shared<SectionAttributeAppender>();
    if (mainAppender->Init(schema)) {
        _mainDocAppender = mainAppender;
    }
    // IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    // if (subSchema) {
    //     SectionAttributeAppenderPtr subAppender(new SectionAttributeAppender());
    //     if (subAppender->Init(subSchema)) {
    //         _subDocAppender = subAppender;
    //     }
    // }
    // return _mainDocAppender || _subDocAppender;
    return _mainDocAppender != nullptr;
}

Status SectionAttributeRewriter::RewriteOneDoc(const shared_ptr<NormalDocument>& doc)
{
    DocOperateType opType = doc->GetDocOperateType();
    if (opType != ADD_DOC) {
        return Status::OK();
    }
    if (_mainDocAppender) {
        auto status = RewriteIndexDocument(_mainDocAppender, doc);
        RETURN_IF_STATUS_ERROR(status, "rewrite index document failed");
    }

    // if (_subDocAppender) {
    //     const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    //     for (size_t i = 0; i < subDocs.size(); ++i) {
    //         RewriteIndexDocument(_subDocAppender, subDocs[i]);
    //     }
    // }

    return Status::OK();
}

Status SectionAttributeRewriter::RewriteIndexDocument(const std::shared_ptr<SectionAttributeAppender>& appender,
                                                      const std::shared_ptr<NormalDocument>& document)
{
    const auto& indexDocument = document->GetIndexDocument();
    if (!indexDocument) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "indexDocument is NULL!");
    }
    return appender->AppendSectionAttribute(indexDocument);
}
}} // namespace indexlibv2::document
