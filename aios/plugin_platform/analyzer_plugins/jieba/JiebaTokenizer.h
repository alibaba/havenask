#pragma once

#include "build_service/analyzer/Tokenizer.h"
#include "cppjieba/Jieba.hpp"
#include <set>
#include "util/Log.h"

namespace pluginplatform {
namespace analyzer_plugins {

class JiebaTokenizer : public build_service::analyzer::Tokenizer
{
public:
    JiebaTokenizer();
    JiebaTokenizer(std::string dictPath, std::string hmmPath, std::string userDictPath, std::string idfPath, std::string stopWordPath, std::set<std::string> stopWords);
    ~JiebaTokenizer();
private:
    JiebaTokenizer& operator=(const JiebaTokenizer &);
    void reset();
    bool getAndCheckJiebaDataPath(const build_service::KeyValueMap &parameters, const build_service::config::ResourceReaderPtr &resourceReader, std::string key, std::string& path);
public:
    bool init(const build_service::KeyValueMap &parameters,
              const build_service::config::ResourceReaderPtr &resourceReader);
    void tokenize(const char *text, size_t len);
    bool next(build_service::analyzer::Token &token);
    Tokenizer *clone();

private:
    cppjieba::Jieba* jieba;
    size_t _cursor;
    std::set<std::string> _stopWords;
    std::vector<std::string> _cutWords;
    std::string _dictPath, _hmmPath, _userDictPath, _idfPath, _stopWordPath;
    PLUG_LOG_DECLARE();
};

}
}
