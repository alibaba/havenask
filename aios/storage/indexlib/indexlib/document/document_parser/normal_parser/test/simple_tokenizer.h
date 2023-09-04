#ifndef __INDEXLIB_TEST_SIMPLETOKENIZER_H
#define __INDEXLIB_TEST_SIMPLETOKENIZER_H

#include "indexlib/common_define.h"
#include "indexlib/document/extend_document/tokenize/analyzer_token.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class SimpleTokenizer
{
public:
    SimpleTokenizer(const std::string& delimiter = "\t");
    ~SimpleTokenizer();

public:
    void tokenize(const char* text, size_t len);
    bool next(AnalyzerToken& token);
    void reset();

private:
    std::string _delimiter;
    std::vector<size_t> _posVec;
    const char* _text;
    size_t _position;
    size_t _curPos;
    size_t _textLen;
    const char* _begin;
    const char* _end;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SimpleTokenizer);
}} // namespace indexlib::document

#endif //__INDEXLIB_TEST_SIMPLETOKENIZER_H
