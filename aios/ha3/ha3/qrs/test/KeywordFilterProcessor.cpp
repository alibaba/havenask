#include <ha3/qrs/test/KeywordFilterProcessor.h>

using namespace std;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, KeywordFilterProcessor);

KeywordFilterProcessor::KeywordFilterProcessor() { 
}

KeywordFilterProcessor::~KeywordFilterProcessor() { 
}

void KeywordFilterProcessor::process(RequestPtr &requestPtr, 
                                     ResultPtr &resultPtr) 
{
    HA3_LOG(TRACE3, "process in KeywordFilterProcessor");
    string keywordList[] = {
        "中国",
        "日本",
        "篮球",
        "老虎"
    };
    string oriString = requestPtr->getOriginalString();
    for (size_t i = 0; i < sizeof(keywordList)/sizeof(string); i++) {
        replaceString(oriString, keywordList[i], "");
    }
    requestPtr->setOriginalString(oriString);
    QrsProcessor::process(requestPtr, resultPtr);
}

QrsProcessorPtr KeywordFilterProcessor::clone() {
    QrsProcessorPtr ptr(new KeywordFilterProcessor(*this));
    return ptr;
}

string KeywordFilterProcessor::getName() const {
    return "KeywordFilterProcessor";
}

void replaceString(string &oriString, const string &from, const string &to) {
    size_t found = oriString.find(from);
    while(found != string::npos) {
        oriString.replace(found, from.size(), to);
        found = oriString.find(from);
    }
}
END_HA3_NAMESPACE(qrs);

