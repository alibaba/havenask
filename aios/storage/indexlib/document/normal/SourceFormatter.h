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
#include "autil/Span.h"
#include "indexlib/base/Status.h"
namespace indexlibv2 { namespace config {
class SourceIndexConfig;
}} // namespace indexlibv2::config
namespace autil::mem_pool {
class Pool;
}
namespace indexlib { namespace document {
class GroupFieldFormatter;
class SerializedSourceDocument;
class SourceDocument;
}} // namespace indexlib::document
namespace indexlibv2 { namespace document {

class SourceFormatter
{
public:
    SourceFormatter() = default;
    SourceFormatter(const SourceFormatter& other) = delete;
    ~SourceFormatter() = default;

public:
    void Init(const std::shared_ptr<config::SourceIndexConfig>& sourceConfig);

    Status SerializeSourceDocument(const std::shared_ptr<indexlib::document::SourceDocument>& document,
                                   autil::mem_pool::Pool* pool,
                                   std::shared_ptr<indexlib::document::SerializedSourceDocument>& serDoc);

    Status DeserializeSourceDocument(const indexlib::document::SerializedSourceDocument* serDoc,
                                     indexlib::document::SourceDocument* document);
    Status DeserializeSourceDocument(
        indexlib::document::SerializedSourceDocument* serDoc,
        const std::function<Status(const autil::StringView&, const autil::StringView&)>& callback);

private:
    Status SerializeData(const std::shared_ptr<indexlib::document::SourceDocument>& document,
                         autil::mem_pool::Pool* pool,
                         std::shared_ptr<indexlib::document::SerializedSourceDocument>& serDoc);

    Status SerializeMeta(const std::shared_ptr<indexlib::document::SourceDocument>& document,
                         autil::mem_pool::Pool* pool,
                         std::shared_ptr<indexlib::document::SerializedSourceDocument>& serDoc);

    Status SerializeAccessary(const std::shared_ptr<indexlib::document::SourceDocument>& document,
                              autil::mem_pool::Pool* pool,
                              std::shared_ptr<indexlib::document::SerializedSourceDocument>& serDoc);

    void Reset();

private:
    std::vector<std::shared_ptr<indexlib::document::GroupFieldFormatter>> _groupFormatterVec;
    std::shared_ptr<indexlib::document::GroupFieldFormatter> _metaFormatter;
    std::shared_ptr<indexlib::document::GroupFieldFormatter> _accessaryFormatter;
    size_t _groupCount = 0;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////
}} // namespace indexlibv2::document
