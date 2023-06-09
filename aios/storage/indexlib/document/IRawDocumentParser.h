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

#include <map>
#include <string>

#include "autil/NoCopyable.h"

namespace indexlibv2::document {

class RawDocument;

class IRawDocumentParser : private autil::NoCopyable
{
public:
    struct Message {
        std::string data;
        int64_t timestamp = 0;
        uint16_t hashId = 0;
    };

public:
    virtual ~IRawDocumentParser() {}

public:
    virtual bool Init(const std::map<std::string, std::string>& kvMap) = 0;
    virtual bool Parse(const Message& msg, RawDocument& rawDoc) = 0;
};

} // namespace indexlibv2::document
