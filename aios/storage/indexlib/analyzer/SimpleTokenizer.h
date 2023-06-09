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
#include "indexlib/analyzer/ITokenizer.h"

namespace indexlibv2 { namespace analyzer {

class SimpleTokenizer : public ITokenizer
{
public:
    using Token = indexlib::document::AnalyzerToken;

    explicit SimpleTokenizer(const std::string& delimiter = "\t");
    virtual ~SimpleTokenizer() = default;

    void tokenize(const char* text, size_t len) override;
    bool next(Token& token) override;
    ITokenizer* clone() override;
    void reset();

    const std::string& GetDelimiter() const { return _delimiter; }
    void SetDelimiter(const std::string& delimiter) { _delimiter = delimiter; }

private:
    std::string _delimiter;
    std::vector<size_t> _posVec;
    const char* _text;
    size_t _position;
    size_t _curPos;
    size_t _textLen;
    const char* _begin;
    const char* _end;

    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::analyzer
