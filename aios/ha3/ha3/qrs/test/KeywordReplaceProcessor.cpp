#include <ha3/qrs/test/KeywordReplaceProcessor.h>

using namespace std;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, KeywordReplaceProcessor);

KeywordReplaceProcessor::KeywordReplaceProcessor() { 
}

KeywordReplaceProcessor::~KeywordReplaceProcessor() { 
}
void KeywordReplaceProcessor::process(RequestPtr &requestPtr, 
                                     ResultPtr &resultPtr) 
{
    HA3_LOG(TRACE3, "process in KeywordReplaceProcessor");
    string keywordList[] = {
        "boy", "girl", //Replace "boy" to "girl"
        "酱油", "汽油",
        "黄色", "白色",
        "黄瑞华", "黄锐华",
        "黄日华", "黄锐华",
        "huangrh", "黄锐华",
        "huangruihua", "黄锐华"
    };
    string oriString = requestPtr->getOriginalString();
    for (size_t i = 0; i < sizeof(keywordList)/sizeof(string)/2; i++ ) {
        replaceString(oriString, keywordList[2*i], keywordList[2*i + 1]);
    }
    requestPtr->setOriginalString(oriString);
    QrsProcessor::process(requestPtr, resultPtr);
}

QrsProcessorPtr KeywordReplaceProcessor::clone() {
    QrsProcessorPtr ptr(new KeywordReplaceProcessor(*this));
    return ptr;
}

string KeywordReplaceProcessor::getName() const {
    return "KeywordReplaceProcessor";
}
END_HA3_NAMESPACE(qrs);

