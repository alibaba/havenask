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
#include "indexlib/base/Types.h"
#include "indexlib/document/document_rewriter/IDocumentRewriter.h"

namespace indexlibv2::index {
class AttributeConvertor;
class AttributeConfig;
} // namespace indexlibv2::index
namespace indexlibv2::document {

class TTLSetter : public IDocumentRewriter
{
public:
    TTLSetter(const std::shared_ptr<index::AttributeConfig>& ttlAttributeConfig, uint32_t defalutTTLInSeconds);
    ~TTLSetter();

public:
    Status Rewrite(document::IDocumentBatch* batch) override;

private:
    fieldid_t _ttlFieldId;
    uint32_t _defalutTTLInSeconds;
    std::string _ttlFieldName;
    std::shared_ptr<index::AttributeConvertor> _ttlConvertor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
