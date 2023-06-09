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
#include "indexlib/document/orc/OrcDocumentParser.h"

#include "indexlib/document/orc/OrcSingleDocumentParser.h"

namespace indexlib::document {
AUTIL_LOG_SETUP(indexlib.document, OrcDocumentParser);

OrcDocumentParser::OrcDocumentParser(const std::shared_ptr<indexlibv2::analyzer::IAnalyzerFactory>& analyzerFactory,
                                     bool needParseRawDoc)
    : indexlibv2::document::NormalDocumentParser(analyzerFactory, needParseRawDoc)
{
}

OrcDocumentParser::~OrcDocumentParser() {}

std::unique_ptr<indexlibv2::document::SingleDocumentParser> OrcDocumentParser::CreateSingleDocumentParser() const
{
    return std::make_unique<OrcSingleDocumentParser>();
}

} // namespace indexlib::document
