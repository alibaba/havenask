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
#include "indexlib/document/extractor/plain/SourceDocInfoExtractor.h"

#include <memory>

#include "indexlib/document/IDocument.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"

namespace indexlibv2::plain {
AUTIL_LOG_SETUP(indexlib.plain, SourceDocInfoExtractor);

SourceDocInfoExtractor::SourceDocInfoExtractor(const std::shared_ptr<config::IIndexConfig>& indexConfig)
{
    _sourceIndexConfig = std::dynamic_pointer_cast<config::SourceIndexConfig>(indexConfig);
}

Status SourceDocInfoExtractor::ExtractField(document::IDocument* doc, void** field)
{
    indexlibv2::document::NormalDocument* normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    if (!normalDoc) {
        AUTIL_LOG(ERROR, "can not extract attribute information from the given document.");
        return Status::Unknown("can not extract attribute information from the given document.");
    }
    const auto& sourceDoc = normalDoc->GetSourceDocument();
    *field = (void*)sourceDoc.get();
    return Status::OK();
}

bool SourceDocInfoExtractor::IsValidDocument(document::IDocument* doc)
{
    assert(_sourceIndexConfig != nullptr);

    indexlibv2::document::NormalDocument* normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    if (!normalDoc) {
        return false;
    }
    if (doc->GetDocOperateType() != ADD_DOC) {
        return true;
    }
    const auto& sourceDoc = normalDoc->GetSourceDocument();
    if (!sourceDoc) {
        return false;
    }
    return true;
}

} // namespace indexlibv2::plain
