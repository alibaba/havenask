#include "build_service/analyzer/IdleTokenizer.h"

namespace build_service {
namespace analyzer {
BS_LOG_SETUP(analyzer, IdleTokenizer);

IdleTokenizer::IdleTokenizer() {
    _firstToken = true;
    _text = NULL;
}

IdleTokenizer::~IdleTokenizer() {
}

void IdleTokenizer::tokenize(const char *text, size_t len) {
    _text = text;
    _len = len;
    _firstToken = true;
}

bool IdleTokenizer::next(Token &token) {
    if (!_firstToken || _len == 0) {
        return false;
    }

    token.getNormalizedText().assign(_text, _text + _len);
    token.setPosition(0);
    token.setIsRetrieve(true);
    _firstToken = false;
    return true;
}

Tokenizer* IdleTokenizer::clone() {
    return new IdleTokenizer();
}

}
}
