#ifndef ISEARCH_BS_SIMPLETOKENIZER_H
#define ISEARCH_BS_SIMPLETOKENIZER_H

#include "build_service/util/Log.h"
#include "build_service/analyzer/Tokenizer.h"

namespace build_service {
namespace analyzer {

class SimpleTokenizer : public Tokenizer
{
public:
    SimpleTokenizer(const std::string &delimiter = "\t");
    virtual ~SimpleTokenizer();
public:
    bool init(const KeyValueMap &parameters,
              const config::ResourceReaderPtr &resourceReaderPtr);
    void tokenize(const char *text, size_t len);
    bool next(Token &token);
    Tokenizer* clone();
    void reset();
private:
    std::string _delimiter;
    std::vector<size_t> _posVec;
    const char *_text;
    size_t _position;
    size_t _curPos;
    size_t _textLen;
    const char* _begin;
    const char* _end;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_SIMPLETOKENIZER_H
