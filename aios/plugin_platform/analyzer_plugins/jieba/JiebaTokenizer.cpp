#include "JiebaTokenizer.h"
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <build_service/util/FileUtil.h>
#include "util/Log.h"

using namespace std;
using namespace autil;
using namespace build_service;
using namespace build_service::analyzer;
using namespace build_service::config;
using namespace build_service::util;

namespace pluginplatform {
namespace analyzer_plugins {
BS_LOG_SETUP(analyzer, JiebaTokenizer);

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
        fullPath = configPath + (configPath[configPath.size()-1] == '/'? path:("/"+path));
        if(!FileUtil::isExist(fullPath.c_str(), flag) || !flag) {
            PLUG_LOG(ERROR, "jieba tokenizer relative file not exists: %s", fullPath.c_str());
            return false;
        }
        return true;
    }
    else {
        PLUG_LOG(ERROR, "jieba tokenizer require parameter %s", key.c_str());
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
    resourceReader->getFileContent(stopWordContent, parameters.find(STOP_WORD_PATH_KEY)->second);
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
