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
#include "indexlib/document/kkv/KKVDocumentParser.h"

#include "indexlib/document/kkv/KKVIndexDocumentParser.h"
#include "indexlib/document/kv/KVIndexDocumentParser.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kv/Common.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, KKVDocumentParser);

KKVDocumentParser::KKVDocumentParser() = default;

KKVDocumentParser::~KKVDocumentParser() = default;

std::unique_ptr<KVIndexDocumentParserBase>
KKVDocumentParser::CreateIndexFieldsParser(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    const auto& indexType = indexConfig->GetIndexType();
    if (indexType == index::KV_INDEX_TYPE_STR) {
        return std::make_unique<KVIndexDocumentParser>(_counter);
    } else if (indexType == index::KKV_INDEX_TYPE_STR) {
        return std::make_unique<KKVIndexDocumentParser>(_counter);
    } else {
        AUTIL_LOG(ERROR, "unexpected index[%s:%s] in kkv table", indexConfig->GetIndexName().c_str(),
                  indexConfig->GetIndexType().c_str());
        return nullptr;
    }
}

} // namespace indexlibv2::document
