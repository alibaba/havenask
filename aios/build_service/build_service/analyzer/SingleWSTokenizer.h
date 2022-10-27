#ifndef ISEARCH_BS_SINGLEWSTOKENIZER_H
#define ISEARCH_BS_SINGLEWSTOKENIZER_H

#include "build_service/util/Log.h"
#include "build_service/analyzer/Tokenizer.h"
#include "build_service/analyzer/SingleWSScanner.h"

namespace build_service {
namespace analyzer {

class SingleWSTokenizer : public Tokenizer
{
public:
    SingleWSTokenizer();
    ~SingleWSTokenizer();
public:
    bool init(const KeyValueMap &parameters,
                      const config::ResourceReaderPtr &resourceReaderPtr) {
        return true;
    }
    void tokenize(const char *text, size_t len);
    bool next(Token &token);
    Tokenizer* clone();
private:
    void clear();
private:
    SingleWSScanner *_scanner;
    std::istringstream *_iss;
    const char* _pToken;
    int _tokenLen;
    uint32_t _position;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleWSTokenizer);

}
}

#endif //ISEARCH_BS_SINGLEWSTOKENIZER_H
