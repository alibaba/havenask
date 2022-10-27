#ifndef ISEARCH_BS_STOPWORDFILTER_H
#define ISEARCH_BS_STOPWORDFILTER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <set>

namespace build_service {
namespace analyzer {

class StopWordFilter
{
public:
    StopWordFilter(const std::set<std::string> &stopWords);
    StopWordFilter();
    virtual ~StopWordFilter();
public:
    bool isStopWord(const std::string &word);
    void setStopWords(const std::set<std::string> &stopWords);
private:
    std::set<std::string> _stopWords;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(StopWordFilter);

}
}

#endif //ISEARCH_BS_STOPWORDFILTER_H
