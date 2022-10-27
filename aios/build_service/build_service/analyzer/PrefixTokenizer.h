#ifndef ISEARCH_BS_PREFIXTOKENIZER_H
#define ISEARCH_BS_PREFIXTOKENIZER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/analyzer/Tokenizer.h"
#include "build_service/analyzer/PrefixTokenizerImpl.h"

namespace build_service {
namespace analyzer {

class PrefixTokenizer : public Tokenizer
{
public:
    PrefixTokenizer();
    PrefixTokenizer(uint32_t maxTokenNum, bool includeSelf,
                    const std::string &seperator=std::string(1, MULTI_VALUE_SEPARATOR));
    ~PrefixTokenizer();
private:
    PrefixTokenizer& operator=(const PrefixTokenizer &);
public:
    bool init(const KeyValueMap &parameters,
              const config::ResourceReaderPtr &resourceReader);
    void tokenize(const char *text, size_t len);
    bool next(Token &token);
    Tokenizer *clone();
private:
    void clear();
private:
    uint32_t _maxTokenNum;
    bool _includeSelf;
    std::string _seperator;

    const char *_text;
    size_t _position;
    std::vector<PrefixTokenizerImpl*> _impls;
    size_t _cursor;
private:
    static const uint32_t DEFAULT_MAX_TOKEN_NUM;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PrefixTokenizer);

}
}

#endif //ISEARCH_BS_PREFIXTOKENIZER_H
