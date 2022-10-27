#ifndef ISEARCH_BS_SUFFIXPREFIXTOKENIZER_H
#define ISEARCH_BS_SUFFIXPREFIXTOKENIZER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/analyzer/Tokenizer.h"
#include "build_service/analyzer/SuffixPrefixTokenizerImpl.h"
#include "build_service/analyzer/PrefixTokenizerImpl.h"

namespace build_service {
namespace analyzer {

class SuffixPrefixTokenizer : public Tokenizer
{
public:
    SuffixPrefixTokenizer();
    SuffixPrefixTokenizer(uint32_t maxSuffixLen,
                    uint32_t maxPrefixLen,
                    const std::string &seperator=std::string(1, MULTI_VALUE_SEPARATOR));
    ~SuffixPrefixTokenizer();
private:
    SuffixPrefixTokenizer& operator=(const SuffixPrefixTokenizer &);
public:
    bool init(const KeyValueMap &parameters,
              const config::ResourceReaderPtr &resourceReader);
    void tokenize(const char *text, size_t len);
    bool next(Token &token);
    Tokenizer *clone();
private:
    void clear();
private:
    uint32_t _maxSuffixLen;
    std::string _seperator;

    const char *_text;
    size_t _position;
    std::vector<PrefixTokenizerImpl*> _impls;
    size_t _cursor;
    uint32_t _maxPrefixLen;
    std::vector<std::string> _suffixStrs;

private:
    static const uint32_t DEFAULT_SUFFIX_LEN;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SuffixPrefixTokenizer);

}
}

#endif //ISEARCH_BS_SUFFIXPREFIXTOKENIZER_H
