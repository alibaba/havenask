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

#include <set>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/document/IIndexFieldsParser.h"

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::document {

class PlainDocumentParser : public IDocumentParser
{
public:
    PlainDocumentParser() = default;
    ~PlainDocumentParser() = default;

public:
    Status Init(const std::shared_ptr<config::TabletSchema>& schema,
                const std::shared_ptr<DocumentInitParam>& initParam) override;

    std::unique_ptr<IDocumentBatch> Parse(const std::string& docString, const std::string& docFormat) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>> Parse(ExtendDocument& extendDoc) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>> Parse(const autil::StringView& serializedData) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>>
    ParseRawDoc(const std::shared_ptr<RawDocument>& rawDoc) const override;

private:
    std::set<std::string> _indexTypes;
    std::unordered_map<autil::StringView, std::unique_ptr<IIndexFieldsParser>> _indexFieldsParsers;
    schemaid_t _schemaId = DEFAULT_SCHEMAID;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
