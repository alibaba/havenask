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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/document/raw_document_parser.h"

namespace build_service { namespace reader {

class DummyDocParser : public indexlib::document::RawDocumentParser
{
public:
    DummyDocParser() = default;
    ~DummyDocParser() = default;

    DummyDocParser(const DummyDocParser&) = delete;
    DummyDocParser& operator=(const DummyDocParser&) = delete;
    DummyDocParser(DummyDocParser&&) = delete;
    DummyDocParser& operator=(DummyDocParser&&) = delete;

public:
    bool parse(const std::string& docString, indexlib::document::RawDocument& rawDoc) override
    {
        assert(false);
        return false;
    }
};

BS_TYPEDEF_PTR(DummyDocParser);

}} // namespace build_service::reader
