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

#include "indexlib/document/normal/tokenize/AnalyzerToken.h"

namespace indexlibv2 { namespace analyzer {

class ITokenizer
{
public:
    using Token = indexlib::document::AnalyzerToken;

    ITokenizer() {}
    virtual ~ITokenizer() {}

    virtual void tokenize(const char* text, size_t len) = 0;
    virtual bool next(Token& token) = 0;
    virtual ITokenizer* clone() = 0;
    virtual void destroy() { delete this; }

public:
    // for ha3
    virtual ITokenizer* allocate() { return clone(); }
    virtual void deallocate() { destroy(); }
};

}} // namespace indexlibv2::analyzer
