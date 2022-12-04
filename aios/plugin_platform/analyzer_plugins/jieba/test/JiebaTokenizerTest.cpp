#include <string>
#include <unittest/unittest.h>

#include "build_service/analyzer/Token.h"
#include "jieba/JiebaTokenizer.h"

using namespace std;
using namespace autil;
using namespace build_service;
using namespace build_service::analyzer;
using namespace build_service::config;
using namespace build_service::util;

namespace pluginplatform {
namespace analyzer_plugins {

class JiebaTokenizerTest : public TESTBASE  {
public:
    void setUp();
    void tearDown();
protected:
    void testTokenizeEmptyString();

protected:
    JiebaTokenizer* _tokenizer;
    Token _token;
};


void JiebaTokenizerTest::setUp() {
    _tokenizer = new JiebaTokenizer();
    string testDataPath = GET_TEST_DATA_PATH();
    printf("test Data path %s\n", testDataPath.c_str());
    ResourceReaderPtr reader(new ResourceReader(testDataPath));
    KeyValueMap map;
    map["dict_path"] = "jieba_dict/jieba.dict.utf8";
    map["hmm_path"] = "jieba_dict/hmm_model.utf8";
    map["user_dict_path"] = "jieba_dict/user.dict.utf8";
    map["idf_path"] = "jieba_dict/idf.utf8";
    map["stop_word_path"] = "jieba_dict/stop_words.utf8";
    ASSERT_TRUE(_tokenizer->init(map, reader));
}

void JiebaTokenizerTest::tearDown() {
    DELETE_AND_SET_NULL(_tokenizer);
}

TEST_F(JiebaTokenizerTest, testTokenize) {
    string str("他来到了，网易杭研大厦。");
    vector<std::pair<string, bool>> results;
    results.push_back(std::make_pair("他", false));
    results.push_back(std::make_pair("来到", true));
    results.push_back(std::make_pair("了", false));
    results.push_back(std::make_pair("，", false));
    results.push_back(std::make_pair("网易", true));
    results.push_back(std::make_pair("杭研", true));
    results.push_back(std::make_pair("大厦", true));
    results.push_back(std::make_pair("。", false));
    _tokenizer->tokenize(str.data(), str.size());
    for(size_t i=0;i<results.size();i++){
        ASSERT_TRUE(_tokenizer->next(_token));
        ASSERT_TRUE(_token.getNormalizedText() == results[i].first);
        ASSERT_TRUE(_token.isRetrieve() == results[i].second);
    }
}


}
}
