#include "indexlib/document/normal/test/SimpleTokenizer.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace indexlibv2 { namespace document {

AUTIL_LOG_SETUP(indexlib.document, SimpleTokenizer);

SimpleTokenizer::SimpleTokenizer() : SimpleTokenizer("\t") {}

SimpleTokenizer::SimpleTokenizer(const string& delimiter)
{
    _delimiter = delimiter;
    reset();
}

SimpleTokenizer::~SimpleTokenizer() {}

void SimpleTokenizer::reset()
{
    _curPos = 0;
    _position = 0;
    _text = NULL;
    _textLen = 0;
    _begin = NULL;
    _end = NULL;
}

void SimpleTokenizer::tokenize(const char* text, size_t textLen)
{
    _text = text;
    _textLen = textLen;

    StringUtil::sundaySearch(_text, _textLen, _delimiter.c_str(), _delimiter.size(), _posVec);
    _curPos = 0;
    _position = 0;
    _begin = _text;
    if (!_posVec.empty()) {
        _end = _text + _posVec[0];
    } else {
        _end = _text + _textLen;
    }
}

bool SimpleTokenizer::next(indexlib::document::AnalyzerToken& token)
{
    if (_begin >= _text + _textLen) {
        return false;
    }

    if (_begin != _end) {
        token.getNormalizedText().assign(_begin, _end);
        token.setPosition(_position++);
        _begin = _end;
        token.setIsDelimiter(false);
        token.setIsRetrieve(true);
    } else {
        token.getNormalizedText().assign(_delimiter);
        token.setPosition(_position++);
        token.setIsDelimiter(true);
        token.setIsRetrieve(false);
        _begin += _delimiter.size();
        ++_curPos;
        if (_curPos >= _posVec.size()) {
            _end = _text + _textLen;
        } else {
            _end = _text + _posVec[_curPos];
        }
    }
    return true;
}
}} // namespace indexlibv2::document
