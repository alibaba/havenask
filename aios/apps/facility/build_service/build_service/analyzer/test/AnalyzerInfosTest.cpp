#include "build_service/analyzer/AnalyzerInfos.h"

#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/codec/NormalizeOptions.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/analyzer/TokenizerConfig.h"
#include "build_service/common_define.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "indexlib/analyzer/AnalyzerDefine.h"
#include "indexlib/analyzer/AnalyzerInfo.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil::legacy;
using namespace build_service::plugin;
using namespace indexlibv2::analyzer;
using autil::codec::NormalizeOptions;

namespace build_service { namespace analyzer {

class AnalyzerInfosTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    void checkAnalyzerInfo(AnalyzerInfos& analyzerInfos);
};

void AnalyzerInfosTest::setUp() {}

void AnalyzerInfosTest::tearDown() {}

TEST_F(AnalyzerInfosTest, testSerializeAndDeserialize)
{
    BS_LOG(DEBUG, "Begin Test!");
    string analyzerConfigString = "{\
    \"analyzers\":                                              \
    {                                                           \
      \"taobao_analyzer\":                                      \
	{                                                       \
	    \"tokenizer_configs\" :                             \
	    {                                                   \
		\"tokenizer_type\" : \"aliws\",                 \
		\"tokenizer_id\" : \"TAOBAO_CHN\"               \
	    },                                                  \
	    \"stopwords\" : [],                                 \
	    \"normalize_options\" :                             \
	    {                                                   \
		\"case_sensitive\" : false,                     \
		\"traditional_sensitive\" : true,               \
		\"width_sensitive\" : true                      \
	    }                                                   \
	},                                                      \
     \"internet_analyzer\" :                                    \
	{                                                       \
	    \"tokenizer_configs\" :                             \
	    {                                                   \
		\"tokenizer_type\" : \"aliws\",                 \
		\"tokenizer_id\" : \"INTERNET_CHN\"             \
	    },                                                  \
	    \"stopwords\" : [\" \", \",\"],                     \
	    \"normalize_options\" :                             \
	    {                                                   \
		\"case_sensitive\" : true,                      \
		\"traditional_sensitive\" : true,               \
		\"width_sensitive\" : true                      \
	    }                                                   \
	},                                                      \
     \"my_analyzer\" :                                          \
	{                                                       \
	    \"tokenizer_name\" : \"MyTokenizer1\",              \
	    \"stopwords\" : [\" \", \",\"]                     \
	}                                                       \
    },                                                          \
    \"tokenizer_config\" : {                                    \
	\"modules\" : [                                         \
            {                                                   \
                \"module_name\": \"TokenizerModule1\",          \
                \"module_path\": \"libTokenizerModule1.so\",    \
                \"parameters\": {                              \
                }                                               \
            },                                                  \
            {                                                   \
                \"module_name\": \"TokenizerModule2\",          \
                \"module_path\": \"libTokenizerModule2.so\",    \
                \"parameters\": {                              \
                }                                               \
            }                                                   \
        ],                                                      \
	\"tokenizers\" : [                                      \
	    {                                                   \
		\"tokenizer_name\" : \"MyTokenizer1\",          \
		\"tokenizer_type\" : \"MyTokenizer1Type\",      \
		\"module_name\"    : \"TokenizerModule1\",      \
		\"parameters\": {                              \
		    \"key1\" : \"value1\",                      \
		    \"key2\" : \"value2\"                       \
                }                                               \
	    },                                                  \
	    {                                                   \
		\"tokenizer_name\" : \"MyTokenizer2\",          \
		\"tokenizer_type\" : \"MyTokenizer1Type\",      \
		\"module_name\"    : \"TokenizerModule1\",      \
		\"parameters\": {                              \
		    \"key3\" : \"value3\",                      \
		    \"key4\" : \"value4\"                       \
                }                                               \
	    },                                                  \
            {                                                   \
		\"tokenizer_name\" : \"MyTokenizer3\",          \
		\"tokenizer_type\" : \"MyTokenizer2Type\",      \
		\"module_name\"    : \"TokenizerModule2\",      \
		\"parameters\": {                              \
		    \"key5\" : \"value5\"                       \
                }                                               \
	    }                                                   \
	]                                                       \
    }                                                           \
}";

    AnalyzerInfos analyzerInfos;
    FromJsonString(analyzerInfos, analyzerConfigString);
    analyzerInfos.makeCompatibleWithOldConfig();
    checkAnalyzerInfo(analyzerInfos);
    string distStr = ToJsonString(analyzerInfos);
    AnalyzerInfos analyzerInfos2;
    FromJsonString(analyzerInfos2, distStr);
    analyzerInfos2.makeCompatibleWithOldConfig();
    checkAnalyzerInfo(analyzerInfos2);
}

void AnalyzerInfosTest::checkAnalyzerInfo(AnalyzerInfos& analyzerInfos)
{
    ASSERT_EQ((size_t)3, analyzerInfos.getAnalyzerCount());

    const AnalyzerInfo* analyzerInfo1 = analyzerInfos.getAnalyzerInfo("taobao_analyzer");
    ASSERT_TRUE(analyzerInfo1);
    ASSERT_EQ(string("aliws"), analyzerInfo1->getTokenizerConfig("tokenizer_type"));
    ASSERT_EQ(string("TAOBAO_CHN"), analyzerInfo1->getTokenizerConfig("tokenizer_id"));
    ASSERT_EQ(string("TAOBAO_CHN_aliws_"), analyzerInfo1->getTokenizerName());

    ASSERT_EQ((size_t)0, analyzerInfo1->getStopWords().size());
    const NormalizeOptions& normalizeOptions1 = analyzerInfo1->getNormalizeOptions();
    ASSERT_TRUE(!normalizeOptions1.caseSensitive);
    ASSERT_TRUE(normalizeOptions1.traditionalSensitive);
    ASSERT_TRUE(normalizeOptions1.widthSensitive);

    const AnalyzerInfo* analyzerInfo2 = analyzerInfos.getAnalyzerInfo("internet_analyzer");
    ASSERT_TRUE(analyzerInfo2);
    ASSERT_EQ(string("aliws"), analyzerInfo2->getTokenizerConfig("tokenizer_type"));
    ASSERT_EQ(string("INTERNET_CHN"), analyzerInfo2->getTokenizerConfig("tokenizer_id"));
    ASSERT_EQ(string("INTERNET_CHN_aliws_"), analyzerInfo2->getTokenizerName());
    ASSERT_EQ((size_t)2, analyzerInfo2->getStopWords().size());
    const NormalizeOptions& normalizeOptions2 = analyzerInfo2->getNormalizeOptions();
    ASSERT_TRUE(normalizeOptions2.caseSensitive);
    ASSERT_TRUE(normalizeOptions2.traditionalSensitive);
    ASSERT_TRUE(normalizeOptions2.widthSensitive);

    const AnalyzerInfo* analyzerInfo3 = analyzerInfos.getAnalyzerInfo("my_analyzer");
    ASSERT_TRUE(analyzerInfo3);
    ASSERT_EQ(string("MyTokenizer1"), analyzerInfo3->getTokenizerName());

    const TokenizerConfig& tokenizerConfig = analyzerInfos.getTokenizerConfig();
    const ModuleInfos& modules = tokenizerConfig.getModuleInfos();
    const TokenizerInfos& tokenizerInfos = tokenizerConfig.getTokenizerInfos();
    ASSERT_EQ(size_t(2), modules.size());
    ASSERT_EQ(string("TokenizerModule1"), modules[0].moduleName);
    ASSERT_EQ(string("libTokenizerModule1.so"), modules[0].modulePath);
    ASSERT_EQ(string("TokenizerModule2"), modules[1].moduleName);
    ASSERT_EQ(string("libTokenizerModule2.so"), modules[1].modulePath);
    ASSERT_EQ(size_t(5), tokenizerInfos.size());

    ASSERT_EQ(string("MyTokenizer1"), tokenizerInfos[0].getTokenizerName());
    ASSERT_EQ(string("MyTokenizer1Type"), tokenizerInfos[0].getTokenizerType());
    ASSERT_EQ(string("TokenizerModule1"), tokenizerInfos[0].getModuleName());
    ASSERT_EQ(size_t(2), tokenizerInfos[0].getParameters().size());
    KeyValueMap paras = tokenizerInfos[0].getParameters();
    ASSERT_EQ(string("value1"), paras["key1"]);
    ASSERT_EQ(string("value2"), paras["key2"]);

    ASSERT_EQ(string("MyTokenizer2"), tokenizerInfos[1].getTokenizerName());
    ASSERT_EQ(string("MyTokenizer1Type"), tokenizerInfos[1].getTokenizerType());
    ASSERT_EQ(string("TokenizerModule1"), tokenizerInfos[1].getModuleName());
    paras = tokenizerInfos[1].getParameters();
    ASSERT_EQ(size_t(2), paras.size());
    ASSERT_EQ(string("value3"), paras["key3"]);
    ASSERT_EQ(string("value4"), paras["key4"]);

    ASSERT_EQ(string("MyTokenizer3"), tokenizerInfos[2].getTokenizerName());
    ASSERT_EQ(string("MyTokenizer2Type"), tokenizerInfos[2].getTokenizerType());
    ASSERT_EQ(string("TokenizerModule2"), tokenizerInfos[2].getModuleName());
    paras = tokenizerInfos[2].getParameters();
    ASSERT_EQ(size_t(1), paras.size());
    ASSERT_EQ(string("value5"), paras["key5"]);

    ASSERT_EQ(string("INTERNET_CHN_aliws_"), tokenizerInfos[3].getTokenizerName());
    ASSERT_EQ(string("aliws"), tokenizerInfos[3].getTokenizerType());
    ASSERT_EQ(string(""), tokenizerInfos[3].getModuleName());
    paras = tokenizerInfos[3].getParameters();
    ASSERT_EQ(size_t(2), paras.size());
    ASSERT_EQ(string("aliws"), paras["tokenizer_type"]);
    ASSERT_EQ(string("INTERNET_CHN"), paras["tokenizer_id"]);

    ASSERT_EQ(string("TAOBAO_CHN_aliws_"), tokenizerInfos[4].getTokenizerName());
    ASSERT_EQ(string("aliws"), tokenizerInfos[4].getTokenizerType());
    ASSERT_EQ(string(""), tokenizerInfos[4].getModuleName());
    paras = tokenizerInfos[4].getParameters();
    ASSERT_EQ(size_t(2), paras.size());
    ASSERT_EQ(string("aliws"), paras["tokenizer_type"]);
    ASSERT_EQ(string("TAOBAO_CHN"), paras["tokenizer_id"]);
}

TEST_F(AnalyzerInfosTest, testMerge)
{
    AnalyzerInfosPtr analyzerInfosPtr(new AnalyzerInfos());

    {
        AnalyzerInfo simpleInfo;
        simpleInfo.setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
        simpleInfo.setTokenizerConfig(DELIMITER, "\t");
        analyzerInfosPtr->addAnalyzerInfo("alimama", simpleInfo);
    }

    {
        AnalyzerInfo singleInfo;
        singleInfo.setTokenizerConfig(TOKENIZER_TYPE, SINGLEWS_ANALYZER);
        analyzerInfosPtr->addAnalyzerInfo("singlews", singleInfo);
    }

    AnalyzerInfosPtr analyzerInfosPtr2(new AnalyzerInfos());

    {
        AnalyzerInfo simpleInfo;
        simpleInfo.setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
        simpleInfo.setTokenizerConfig(DELIMITER, ";");
        analyzerInfosPtr2->addAnalyzerInfo("alimama", simpleInfo);
    }

    {
        AnalyzerInfo taobaoInfo;
        taobaoInfo.setTokenizerConfig(TOKENIZER_TYPE, ALIWS_ANALYZER);
        taobaoInfo.setTokenizerConfig(TOKENIZER_ID, "TAOBAO_CHN");
        analyzerInfosPtr2->addAnalyzerInfo("taobao", taobaoInfo);
    }

    analyzerInfosPtr->merge(*analyzerInfosPtr2);

    ASSERT_EQ((size_t)3, analyzerInfosPtr->getAnalyzerCount());

    AnalyzerInfos::ConstIterator it = analyzerInfosPtr->begin();

    {
        ASSERT_EQ(string("alimama"), it->first);

        const AnalyzerInfo& analyzerInfo = it->second;
        ASSERT_EQ(string(SIMPLE_ANALYZER), analyzerInfo.getTokenizerConfig(TOKENIZER_TYPE));
        ASSERT_EQ(string("\t"), analyzerInfo.getTokenizerConfig(DELIMITER));
    }

    {
        ASSERT_EQ(string("singlews"), (++it)->first);

        const AnalyzerInfo& analyzerInfo = it->second;
        ASSERT_EQ(string(SINGLEWS_ANALYZER), analyzerInfo.getTokenizerConfig(TOKENIZER_TYPE));
    }

    {
        ASSERT_EQ(string("taobao"), (++it)->first);

        const AnalyzerInfo& analyzerInfo = it->second;
        ASSERT_EQ(string(ALIWS_ANALYZER), analyzerInfo.getTokenizerConfig(TOKENIZER_TYPE));
        ASSERT_EQ(string("TAOBAO_CHN"), analyzerInfo.getTokenizerConfig(TOKENIZER_ID));
    }
}

}} // namespace build_service::analyzer
