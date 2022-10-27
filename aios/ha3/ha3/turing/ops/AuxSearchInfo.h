#ifndef ISEARCH_AUXSEARCHINFO_H
#define ISEARCH_AUXSEARCHINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>

BEGIN_HA3_NAMESPACE(turing);

class AuxSearchInfo
{
public:
    AuxSearchInfo() : _elapsedTime(0), _resultCount(0) {}
    ~AuxSearchInfo() {}
private:
    AuxSearchInfo(const AuxSearchInfo &);
    AuxSearchInfo& operator=(const AuxSearchInfo &);
public:
    std::string toString();
public:
    int64_t _elapsedTime;
    uint32_t _resultCount;
};

HA3_TYPEDEF_PTR(AuxSearchInfo);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_AUXSEARCHINFO_H
