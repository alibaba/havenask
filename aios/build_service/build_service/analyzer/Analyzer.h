#ifndef ISEARCH_BS_ANALYZER_H
#define ISEARCH_BS_ANALYZER_H

#include "build_service/util/Log.h"
#include "build_service/analyzer/Token.h"

namespace build_service {
namespace analyzer {

class TextBuffer;
class Tokenizer;
class Normalizer;
BS_TYPEDEF_PTR(Normalizer);
class StopWordFilter;
BS_TYPEDEF_PTR(StopWordFilter);
class TraditionalTable;
class AnalyzerInfo;

class Analyzer
{
public:
    Analyzer(Tokenizer* tokenizer);
    Analyzer(const Analyzer &analyzer);
    ~Analyzer();
public:
    void init(const AnalyzerInfo *analyzerInfo,
              const TraditionalTable *traditionalTable);
    void tokenize(const char *str, size_t len);
    bool next(Token &token) const;
    Analyzer* clone() const;
    const char* normalize(const char *str, size_t &len);
    void resetTokenizer(Tokenizer* tokenizer);
private:
    NormalizerPtr  _normalizerPtr;
    StopWordFilterPtr  _stopWordFilterPtr;
    Tokenizer* _tokenizer;
    TextBuffer *_textBuffer;
    mutable bool _useNormalizeText;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_ANALYZER_H
