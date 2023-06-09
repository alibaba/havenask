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
#include "autil/NoCopyable.h"
#include "autil/StringView.h"
#include "indexlib/base/Status.h"

namespace indexlibv2::config {
class TabletSchema;
class TabletOptions;
} // namespace indexlibv2::config

namespace indexlibv2::document {
class RawDocument;
class ExtendDocument;
class IDocumentBatch;
class DocumentInitParam;

class IDocumentParser : private autil::NoCopyable
{
public:
    virtual ~IDocumentParser() = default;

public:
    virtual Status Init(const std::shared_ptr<config::TabletSchema>& schema,
                        const std::shared_ptr<DocumentInitParam>& initParam) = 0;

    virtual std::unique_ptr<IDocumentBatch> Parse(const std::string& docString, const std::string& docFormat) const = 0;

    virtual std::pair<Status, std::unique_ptr<IDocumentBatch>> Parse(ExtendDocument& extendDoc) const = 0;

    virtual std::pair<Status, std::unique_ptr<IDocumentBatch>> Parse(const autil::StringView& serializedData) const = 0;

    virtual std::pair<Status, std::unique_ptr<IDocumentBatch>>
    ParseRawDoc(const std::shared_ptr<RawDocument>& rawDoc) const = 0;
};

} // namespace indexlibv2::document
