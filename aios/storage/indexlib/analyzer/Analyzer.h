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
#include "indexlib/document/normal/tokenize/AnalyzerToken.h"

namespace autil { namespace codec {
class Normalizer;
typedef std::shared_ptr<Normalizer> NormalizerPtr;
}} // namespace autil::codec

namespace indexlibv2 { namespace analyzer {

class TextBuffer;
class ITokenizer;

class StopWordFilter;
using StopWordFilterPtr = std::shared_ptr<StopWordFilter>;
class TraditionalTable;
class AnalyzerInfo;

class Analyzer
{
public:
    using Token = indexlib::document::AnalyzerToken;

    explicit Analyzer(ITokenizer* tokenizer);
    Analyzer(const Analyzer& analyzer);
    ~Analyzer();

    void init(const AnalyzerInfo* analyzerInfo, const TraditionalTable* traditionalTable);
    void tokenize(const char* str, size_t len);
    bool next(Token& token) const;
    Analyzer* clone() const;
    const char* normalize(const char* str, size_t& len);
    void resetTokenizer(ITokenizer* tokenizer);

private:
    autil::codec::NormalizerPtr _normalizerPtr;
    StopWordFilterPtr _stopWordFilterPtr;
    ITokenizer* _tokenizer;
    TextBuffer* _textBuffer;
    mutable bool _useNormalizeText;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::analyzer
