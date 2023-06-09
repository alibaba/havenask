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
#include "indexlib/document/normal/rewriter/PackAttributeRewriter.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/rewriter/PackAttributeAppender.h"
#include "indexlib/util/ErrorLogCollector.h"

using namespace std;
using namespace indexlibv2::config;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, PackAttributeRewriter);

PackAttributeRewriter::PackAttributeRewriter() {}

PackAttributeRewriter::~PackAttributeRewriter() {}

pair<Status, bool> PackAttributeRewriter::Init(const shared_ptr<TabletSchema>& schema)
{
    auto mainAppender = make_shared<PackAttributeAppender>();
    auto [status, ret] = mainAppender->Init(schema);
    RETURN2_IF_STATUS_ERROR(status, false, "pack attribute appender init failed");
    if (ret) {
        _mainDocAppender = mainAppender;
    }
    // IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    // if (subSchema) {
    //     MultiRegionPackAttributeAppenderPtr subAppender(new MultiRegionPackAttributeAppender);
    //     if (subAppender->Init(subSchema)) {
    //         _subDocAppender = subAppender;
    //     }
    // }
    return {Status::OK(), _mainDocAppender != nullptr};
}

Status PackAttributeRewriter::RewriteOneDoc(const shared_ptr<NormalDocument>& doc)
{
    if (doc->GetDocOperateType() != ADD_DOC || doc->GetOriginalOperateType() == DELETE_DOC) {
        return Status::OK();
    }
    if (_mainDocAppender) {
        RewriteAttributeDocument(_mainDocAppender, doc);
    }

    // if (_subDocAppender) {
    //     const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    //     for (size_t i = 0; i < subDocs.size(); ++i) {
    //         RewriteAttributeDocument(_subDocAppender, subDocs[i]);
    //     }
    // }
    return Status::OK();
}

void PackAttributeRewriter::RewriteAttributeDocument(const shared_ptr<PackAttributeAppender>& appender,
                                                     const shared_ptr<NormalDocument>& document)
{
    const auto& attributeDocument = document->GetAttributeDocument();
    if (!attributeDocument) {
        return;
    }

    if (!appender->AppendPackAttribute(attributeDocument, document->GetPool())) {
        document->SetDocOperateType(SKIP_DOC);
        AUTIL_LOG(WARN, "AppendPackAttribute fail, skip current doc!");
        ERROR_COLLECTOR_LOG(WARN, "AppendPackAttribute fail, skip current doc!");
    }
}
}} // namespace indexlibv2::document
