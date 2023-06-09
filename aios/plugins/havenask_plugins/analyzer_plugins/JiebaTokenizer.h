/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "build_service/analyzer/Tokenizer.h"
#include "cppjieba/Jieba.hpp"
#include <set>
#include "build_service/util/Log.h"
#include <memory>

namespace pluginplatform {
namespace analyzer_plugins {

class JiebaTokenizer : public build_service::analyzer::Tokenizer
{
public:
    JiebaTokenizer();
    JiebaTokenizer(std::shared_ptr<cppjieba::Jieba> jieba, std::shared_ptr<std::set<std::string>>stopWords);
    ~JiebaTokenizer();
private:
    JiebaTokenizer& operator=(const JiebaTokenizer &);
    void reset();
    bool getAndCheckJiebaDataPath(const build_service::KeyValueMap &parameters, const build_service::config::ResourceReaderPtr &resourceReader, const std::string& key, std::string& path);
public:
    bool init(const build_service::KeyValueMap &parameters,
              const build_service::config::ResourceReaderPtr &resourceReader);
    void tokenize(const char *text, size_t len);
    bool next(build_service::analyzer::Token &token);
    Tokenizer *clone();

private:
    std::shared_ptr<cppjieba::Jieba> _jieba;
    std::shared_ptr<std::set<std::string>> _stopWords;
    size_t _cursor;
    std::vector<std::string> _cutWords;
    std::string _dictPath, _hmmPath, _userDictPath, _idfPath, _stopWordPath;
    BS_LOG_DECLARE();
};

}
}
