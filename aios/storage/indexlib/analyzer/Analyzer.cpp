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
#include "indexlib/analyzer/Analyzer.h"

#include <assert.h>
#include <cstddef>
#include <map>
#include <stdint.h>
#include <string>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/codec/Normalizer.h"
#include "indexlib/analyzer/AnalyzerInfo.h"
#include "indexlib/analyzer/ITokenizer.h"
#include "indexlib/analyzer/StopWordFilter.h"
#include "indexlib/analyzer/TextBuffer.h"
#include "indexlib/analyzer/TraditionalTables.h"

using namespace std;
using namespace autil;
using autil::codec::Normalizer;
using autil::codec::NormalizerPtr;

namespace indexlibv2 { namespace analyzer {
AUTIL_LOG_SETUP(indexlib.analyzer, Analyzer);

Analyzer::Analyzer(ITokenizer* tokenize) : _tokenizer(tokenize)
{
    _textBuffer = new TextBuffer();
    _useNormalizeText = false;
}

Analyzer::Analyzer(const Analyzer& analyzer)
{
    _normalizerPtr = analyzer._normalizerPtr;
    _stopWordFilterPtr = analyzer._stopWordFilterPtr;
    _tokenizer = analyzer._tokenizer->allocate();
    _textBuffer = new TextBuffer();
    _useNormalizeText = false;
}

Analyzer::~Analyzer()
{
    _tokenizer->deallocate();
    _tokenizer = nullptr;
    delete _textBuffer;
}

void Analyzer::init(const AnalyzerInfo* analyzerInfo, const TraditionalTable* traditionalTable)
{
    assert(analyzerInfo);
    const map<uint16_t, uint16_t>* mapTable = traditionalTable ? &traditionalTable->table : nullptr;
    _normalizerPtr.reset(new Normalizer(analyzerInfo->getNormalizeOptions(), mapTable));
    _stopWordFilterPtr.reset(new StopWordFilter(analyzerInfo->getStopWords()));
}

void Analyzer::tokenize(const char* str, size_t len)
{
    const char* normalizedStr = normalize(str, len);
    _tokenizer->tokenize(normalizedStr, len);
}

const char* Analyzer::normalize(const char* str, size_t& len)
{
    if (_normalizerPtr->needNormalize()) {
        _textBuffer->normalize(_normalizerPtr, str, len);
        _useNormalizeText = false;
        return _textBuffer->getNormalizedUTF8Text();
    } else {
        // normalize does not change anything, you can treat normalized text
        // as original text
        _useNormalizeText = true;
        return str;
    }
}

bool Analyzer::next(Token& token) const
{
    token.setIsStopWord(false);
    bool ret = _tokenizer->next(token);
    if (!ret) {
        return false;
    }

    string& normalizeText = token.getNormalizedText();
    if (token.needSetText()) {
        if (_useNormalizeText || !token.isBasicRetrieve()) {
            token.setText(normalizeText);
        } else {
            string& text = token.getText();
            if (!_textBuffer->nextTokenOrignalText(normalizeText, text)) {
                // for protection, if failed to get original text,
                // we will use normalized text as original text
                token.setText(normalizeText);
                _useNormalizeText = true;
            }
        }
    }
    token.setIsStopWord(token.isStopWord() || _stopWordFilterPtr->isStopWord(normalizeText));
    token.setIsSpace(StringUtil::isSpace(normalizeText));
    return true;
}

Analyzer* Analyzer::clone() const { return new Analyzer(*this); }

void Analyzer::resetTokenizer(ITokenizer* tokenizer)
{
    _tokenizer->deallocate();
    _tokenizer = tokenizer;
}

}} // namespace indexlibv2::analyzer
