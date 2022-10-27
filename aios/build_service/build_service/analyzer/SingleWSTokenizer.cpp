#include "build_service/analyzer/SingleWSTokenizer.h"

using namespace std;

namespace build_service {
namespace analyzer {
 BS_LOG_SETUP(analyzer, SingleWSTokenizer);

SingleWSTokenizer::SingleWSTokenizer() {
    _scanner = NULL;
    _iss = NULL;
    _pToken = NULL;
    _tokenLen = 0;
    _position = 0;
}

SingleWSTokenizer::~SingleWSTokenizer() {
    clear();
}

void SingleWSTokenizer::clear() {
    DELETE_AND_SET_NULL(_scanner);
    DELETE_AND_SET_NULL(_iss);
}

void SingleWSTokenizer::tokenize(const char *text, size_t len) {
    clear();
    _iss = new istringstream(string(text, len));
    _scanner = new SingleWSScanner(_iss, NULL);
    _position = 0;
}

bool SingleWSTokenizer::next(Token &token) {
    if (!_scanner) {
        return false;
    }
    while(_scanner->lex(_pToken, _tokenLen)) {
        if (!_pToken) {
            continue;
        }
        token.getNormalizedText().assign(_pToken, _tokenLen);
        token.setIsStopWord(_scanner->isStopWord());
        token.setPosition(_position);
        token.setIsRetrieve(true);
        _position++;
        return true;
    }
    return false;
}

Tokenizer* SingleWSTokenizer::clone() {
    return new SingleWSTokenizer();
}

}
}
