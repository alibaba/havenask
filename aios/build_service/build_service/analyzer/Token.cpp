#include "build_service/analyzer/Token.h"

using namespace std;
namespace build_service {
namespace analyzer {

Token::Token()
    : _text(),
      _normalizedText(),
      _pos((uint32_t)-1),
      _posPayLoad(0)
{
}

Token::Token(const std::string &text, uint32_t pos)
    : _text(text),
      _normalizedText(text),
      _pos(pos),
      _posPayLoad(0)
{
}

Token::Token(const std::string &text, uint32_t pos,
             const std::string &normalizedText, bool isStopWord)
    : _text(text),
      _normalizedText(normalizedText),
      _pos(pos),
      _posPayLoad(0)
{
    _tokenProperty.setStopWord(isStopWord);
}

Token::~Token() {
}

void Token::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_text);
    dataBuffer.write(_normalizedText);
    dataBuffer.write(_pos);
    dataBuffer.write(_tokenProperty);
    dataBuffer.write(_posPayLoad);
}

void Token::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_text);
    dataBuffer.read(_normalizedText);
    dataBuffer.read(_pos);
    dataBuffer.read(_tokenProperty);
    dataBuffer.read(_posPayLoad);
}

}
}
