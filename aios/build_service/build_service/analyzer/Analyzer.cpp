#include "build_service/analyzer/Analyzer.h"
#include "build_service/analyzer/Normalizer.h"
#include "build_service/analyzer/Tokenizer.h"
#include "build_service/analyzer/StopWordFilter.h"
#include "build_service/analyzer/AnalyzerInfos.h"
#include "build_service/analyzer/TextBuffer.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

namespace build_service {
namespace analyzer {
BS_LOG_SETUP(analyzer, Analyzer);

Analyzer::Analyzer(Tokenizer* tokenize)
    : _tokenizer(tokenize)
{
    _textBuffer = new TextBuffer();
    _useNormalizeText = false;
}

Analyzer::Analyzer(const Analyzer &analyzer) {
    _normalizerPtr = analyzer._normalizerPtr;
    _stopWordFilterPtr = analyzer._stopWordFilterPtr;
    _tokenizer = analyzer._tokenizer->allocate();
    _textBuffer = new TextBuffer();
    _useNormalizeText = false;
}

Analyzer::~Analyzer() {
    _tokenizer->deallocate();
    _tokenizer = NULL;
    delete _textBuffer;
}

void Analyzer::init(const AnalyzerInfo *analyzerInfo,
                    const TraditionalTable *traditionalTable)
{
    assert(analyzerInfo);
    const map<uint16_t,uint16_t> *mapTable = traditionalTable
                                             ? &traditionalTable->table
                                             : NULL;
    _normalizerPtr.reset(
            new Normalizer(analyzerInfo->getNormalizeOptions(),mapTable));
    _stopWordFilterPtr.reset(
            new StopWordFilter(analyzerInfo->getStopWords()));
}

void Analyzer::tokenize(const char *str, size_t len) {
    const char *normalizedStr = normalize(str, len);
    _tokenizer->tokenize(normalizedStr, len);
}

const char* Analyzer::normalize(const char *str, size_t& len) {
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

bool Analyzer::next(Token &token) const {
    token.setIsStopWord(false);
    bool ret = _tokenizer->next(token);
    if (!ret) {
        return false;
    }

    string &normalizeText = token.getNormalizedText();
    if (token.needSetText()) {
        if (_useNormalizeText || !token.isBasicRetrieve()) {
            token.setText(normalizeText);
        } else {
            string &text = token.getText();
            if (!_textBuffer->nextTokenOrignalText(normalizeText, text)) {
                // for protection, if failed to get original text,
                // we will use normalized text as original text
                token.setText(normalizeText);
                _useNormalizeText = true;
            }
        }
    }
    token.setIsStopWord(token.isStopWord()
                        || _stopWordFilterPtr->isStopWord(normalizeText));
    token.setIsSpace(StringUtil::isSpace(normalizeText));
    return true;
}

Analyzer* Analyzer::clone() const {
    return new Analyzer(*this);
}

void Analyzer::resetTokenizer(Tokenizer* tokenizer) {
    _tokenizer->deallocate();
    _tokenizer = tokenizer;
}

}
}
