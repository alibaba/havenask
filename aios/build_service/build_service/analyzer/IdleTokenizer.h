#ifndef ISEARCH_BS_IDLETOKENIZER_H
#define ISEARCH_BS_IDLETOKENIZER_H

#include "build_service/util/Log.h"
#include "build_service/analyzer/Tokenizer.h"

namespace build_service {
namespace analyzer {

class IdleTokenizer : public Tokenizer
{
public:
    IdleTokenizer();
    ~IdleTokenizer();
private:
    IdleTokenizer(const IdleTokenizer &);
    IdleTokenizer& operator=(const IdleTokenizer &);
public:
    bool init(const KeyValueMap &parameters,
              const config::ResourceReaderPtr &resourceReaderPtr)
    {
        return true;
    }
    void tokenize(const char *text, size_t len);
    bool next(Token &token);
    Tokenizer* clone();
private:
    bool _firstToken;
    const char *_text;
    size_t _len;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IdleTokenizer);

}
}

#endif //ISEARCH_BS_IDLETOKENIZER_H
