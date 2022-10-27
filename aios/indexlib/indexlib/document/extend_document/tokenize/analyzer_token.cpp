#include "indexlib/document/extend_document/tokenize/analyzer_token.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(document);

AnalyzerToken::AnalyzerToken()
    : _text()
    , _normalizedText()
    , _pos((uint32_t)-1)
    , _posPayLoad(0)
{}

AnalyzerToken::AnalyzerToken(const string &text, uint32_t pos)
    : _text(text)
    , _normalizedText(text)
    , _pos(pos)
    , _posPayLoad(0)
{}

AnalyzerToken::AnalyzerToken(const string &text, uint32_t pos,
                             const string &normalizedText, bool isStopWord)
    : _text(text)
    , _normalizedText(normalizedText)
    , _pos(pos)
    , _posPayLoad(0)
{
    _tokenProperty.setStopWord(isStopWord);
}

AnalyzerToken::~AnalyzerToken() {}

void AnalyzerToken::serialize(DataBuffer &dataBuffer) const {
    dataBuffer.write(_text);
    dataBuffer.write(_normalizedText);
    dataBuffer.write(_pos);
    dataBuffer.write(_tokenProperty);
    dataBuffer.write(_posPayLoad);
}

void AnalyzerToken::deserialize(DataBuffer &dataBuffer) {
    dataBuffer.read(_text);
    dataBuffer.read(_normalizedText);
    dataBuffer.read(_pos);
    dataBuffer.read(_tokenProperty);
    dataBuffer.read(_posPayLoad);
}

IE_NAMESPACE_END(document);
