#pragma once

#include "indexlib/document/normal/tokenize/AnalyzerToken.h"

namespace indexlibv2 { namespace document {

class SimpleTokenizer
{
public:
    SimpleTokenizer();
    SimpleTokenizer(const std::string& delimiter);
    ~SimpleTokenizer();

public:
    void tokenize(const char* text, size_t len);
    bool next(indexlib::document::AnalyzerToken& token);
    void reset();

private:
    std::string _delimiter;
    std::vector<size_t> _posVec;
    const char* _text = nullptr;
    size_t _position = 0;
    size_t _curPos = 0;
    size_t _textLen = 0;
    const char* _begin = nullptr;
    const char* _end = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::document
