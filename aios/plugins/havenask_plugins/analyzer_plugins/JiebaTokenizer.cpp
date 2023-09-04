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
#include "JiebaTokenizer.h"
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include "autil/StringUtil.h"
#include "fslib/fslib.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace build_service;
using namespace build_service::analyzer;
using namespace build_service::config;
using namespace fslib;
using namespace fslib::fs;

namespace pluginplatform {
namespace analyzer_plugins {
AUTIL_LOG_SETUP(analyzer, JiebaTokenizer);

static const string DICT_PATH_KEY = "dict_path";
static const string HMM_PATH_KEY = "hmm_path";
static const string USER_DICT_PATH_KEY = "user_dict_path";
static const string IDF_PATH_KEY = "idf_path";
static const string STOP_WORD_PATH_KEY = "stop_word_path";

JiebaTokenizer::JiebaTokenizer()
{
}

JiebaTokenizer::JiebaTokenizer(shared_ptr<cppjieba::Jieba> jieba, 
                                shared_ptr<std::set<string>>stopWords)
{
    this->_jieba = jieba;
    this->_stopWords = stopWords;
}

JiebaTokenizer::~JiebaTokenizer() {
}

void JiebaTokenizer::reset() {
    _cursor = 0;
    _cutWords.clear();
}

bool JiebaTokenizer::getAndCheckJiebaDataPath(const KeyValueMap &parameters, const ResourceReaderPtr &resourceReader, const string& key, string& fullPath) {
    KeyValueMap::const_iterator it = parameters.find(key);
    if (it != parameters.end()) {
        string path = it->second;
        bool flag = true;
        string configPath = resourceReader->getConfigPath();
        if(StringUtil::startsWith(path, "/")) {
            fullPath = path;
        }
        else {
            fullPath = configPath + (configPath[configPath.size()-1] == '/'? path:("/"+path));
        }
        ErrorCode ec = FileSystem::isExist(fullPath.c_str());
        bool exist = true;
        if (ec != EC_TRUE && ec != EC_FALSE) {
            AUTIL_LOG(WARN,
                    "failed to check whether path:[%s] exist with error:[%s]",
                    fullPath.c_str(),
                    FileSystem::getErrorString(ec).c_str());
            exist = false;
        }
        flag = (ec == EC_TRUE);
        if(!exist || !flag) {
            AUTIL_LOG(ERROR, "jieba tokenizer relative file not exists: %s", fullPath.c_str());
            return false;
        }
        AUTIL_LOG(INFO, "find %s under %s", path.c_str(), fullPath.c_str());
        return true;
    }
    else {
        AUTIL_LOG(ERROR, "jieba tokenizer require parameter %s", key.c_str());
        return false;
    }
}

bool JiebaTokenizer::init(const KeyValueMap &parameters,
                           const ResourceReaderPtr &resourceReader)
{
    string dictPath, hmmPath, userDictPath, idfPath, stopWordPath;
    if(!getAndCheckJiebaDataPath(parameters, resourceReader, DICT_PATH_KEY, _dictPath)) {
        return false;
    }
    if(!getAndCheckJiebaDataPath(parameters, resourceReader, HMM_PATH_KEY, _hmmPath)) {
        return false;
    }
    if(!getAndCheckJiebaDataPath(parameters, resourceReader, USER_DICT_PATH_KEY, _userDictPath)) {
        return false;
    }
    if(!getAndCheckJiebaDataPath(parameters, resourceReader, IDF_PATH_KEY, _idfPath)) {
        return false;
    }
    if(!getAndCheckJiebaDataPath(parameters, resourceReader, STOP_WORD_PATH_KEY, _stopWordPath)) {
        return false;
    }
    _jieba = make_shared<cppjieba::Jieba>(_dictPath, _hmmPath, _userDictPath, _idfPath, _stopWordPath);

    string stopWordContent;
    fslib::util::FileUtil::readFile(_stopWordPath, stopWordContent);
    string tmpWord;
    set<string> stopWords;
    for(size_t i=0;i<stopWordContent.size();i++) {
        char c = stopWordContent[i];
        if(c == '\n' && tmpWord.size() != 0) {
            stopWords.insert(tmpWord);
            tmpWord.clear();
        }
        else {
            tmpWord += c;
        }
    }
    if(tmpWord.size() != 0) {
        stopWords.insert(tmpWord);
    }
    _stopWords = make_shared<set<string>>(stopWords);

    return true;
}

void JiebaTokenizer::tokenize(const char *text, size_t len) {
    reset();
    string testStr = string(text, len);
    _jieba->Cut(testStr, _cutWords, true);
}

bool JiebaTokenizer::next(Token &token) {
    if(_cutWords.size() == 0 || _cursor == _cutWords.size()) {
        return false;
    }
    string word =  _cutWords[_cursor];
    token.getNormalizedText().assign(word.c_str());
    token.setPosition(_cursor++);
    if(_stopWords->find(word) != _stopWords->end()) {
        token.setIsRetrieve(false);
    }
    else {
        token.setIsRetrieve(true);
    }
    return true;
}

Tokenizer *JiebaTokenizer::clone() {
    return new JiebaTokenizer(_jieba, _stopWords);
}

}
}
