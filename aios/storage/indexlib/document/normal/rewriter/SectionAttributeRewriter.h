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

#include "autil/Log.h"
#include "indexlib/document/normal/rewriter/NormalDocumentRewriterBase.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2 { namespace document {
class NormalDocument;
class SectionAttributeAppender;

class SectionAttributeRewriter : public NormalDocumentRewriterBase
{
public:
    SectionAttributeRewriter();
    ~SectionAttributeRewriter();

public:
    bool Init(const std::shared_ptr<config::ITabletSchema>& schema);

private:
    Status RewriteOneDoc(const std::shared_ptr<NormalDocument>& doc) override;
    Status RewriteIndexDocument(const std::shared_ptr<SectionAttributeAppender>& appender,
                                const std::shared_ptr<NormalDocument>& document);

private:
    std::shared_ptr<SectionAttributeAppender> _mainDocAppender;
    std::shared_ptr<SectionAttributeAppender> _subDocAppender;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::document
