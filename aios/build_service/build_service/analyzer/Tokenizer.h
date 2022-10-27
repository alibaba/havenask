#ifndef ISEARCH_BS_TOKENIZER_H
#define ISEARCH_BS_TOKENIZER_H

#include "build_service/util/Log.h"
#include "build_service/analyzer/Token.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/plugin/PooledObject.h"

namespace build_service {
namespace analyzer {

class Tokenizer
{
public:
    Tokenizer() {}
    virtual ~Tokenizer() {}
public:
    virtual bool init(const KeyValueMap &parameters,
                      const config::ResourceReaderPtr &resourceReaderPtr) = 0;
    virtual void tokenize(const char *text, size_t len) = 0;
    virtual bool next(Token &token) = 0;
    virtual Tokenizer* clone() = 0;
    virtual void destroy() {
        delete this;
    }
public:
    // for ha3
    virtual Tokenizer* allocate() {
        return clone();
    }
    virtual void deallocate() {
        destroy();
    }
private:
    BS_LOG_DECLARE();
};

typedef plugin::PooledObject<Tokenizer> PooledTokenizer;

}
}

#endif //ISEARCH_BS_TOKENIZER_H
