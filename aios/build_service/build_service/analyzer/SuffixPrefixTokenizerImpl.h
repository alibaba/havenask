#ifndef ISEARCH_BS_SUFFIXPREFIXTOKENIZERIMPL_H
#define ISEARCH_BS_SUFFIXPREFIXTOKENIZERIMPL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/analyzer/Token.h"

namespace build_service {
namespace analyzer {

class SuffixPrefixTokenizerImpl
{
public:
    SuffixPrefixTokenizerImpl(uint32_t maxSuffixLen);
    ~SuffixPrefixTokenizerImpl();
private:
    SuffixPrefixTokenizerImpl(const SuffixPrefixTokenizerImpl &);
    SuffixPrefixTokenizerImpl& operator=(const SuffixPrefixTokenizerImpl &);
public:
    bool tokenize(const char *text, size_t len);
    bool next(Token &token);
    void reset();
private:
    uint32_t _maxSuffixLen;
    std::vector<size_t> _posVec;
    const char *_text;
    size_t _textLen;
    int32_t _pos_index;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SuffixPrefixTokenizerImpl);

}
}

#endif //ISEARCH_BS_SUFFIXPREFIXTOKENIZERIMPL_H
