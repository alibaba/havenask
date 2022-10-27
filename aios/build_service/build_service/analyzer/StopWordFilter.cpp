#include "build_service/analyzer/StopWordFilter.h"
using namespace std;
using namespace autil;

namespace build_service {
namespace analyzer {
BS_LOG_SETUP(analyzer, StopWordFilter);

StopWordFilter::StopWordFilter(const set<string> &stopWords)
    : _stopWords(stopWords.begin(), stopWords.end()) {}

StopWordFilter::StopWordFilter() {}

StopWordFilter::~StopWordFilter() {}

bool StopWordFilter::isStopWord(const string &word)
{
    return _stopWords.find(word) != _stopWords.end();
}

void StopWordFilter::setStopWords(const set<string> &stopWords) {
    _stopWords.clear();
    _stopWords.insert(stopWords.begin(), stopWords.end());
}

}
}
