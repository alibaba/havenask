#ifndef ISEARCH_BS_PREFIXTOKENIZERIMPL_H
#define ISEARCH_BS_PREFIXTOKENIZERIMPL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/analyzer/Token.h"

namespace build_service {
namespace analyzer {

class PrefixTokenizerImpl
{
public:
    PrefixTokenizerImpl(uint32_t maxTokenNum, bool includeSelf);
    ~PrefixTokenizerImpl();
private:
    PrefixTokenizerImpl(const PrefixTokenizerImpl &);
    PrefixTokenizerImpl& operator=(const PrefixTokenizerImpl &);
public:
    bool tokenize(const char *text, size_t len);
    bool next(Token &token);
    void reset();
private:
    uint32_t _maxTokenNum;
    bool _includeSelf;
    std::vector<size_t> _posVec;
    const char *_text;
    size_t _textLen;
    size_t _position;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PrefixTokenizerImpl);

}
}

#endif //ISEARCH_BS_PREFIXTOKENIZERIMPL_H
