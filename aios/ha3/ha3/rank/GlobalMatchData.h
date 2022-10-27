#ifndef ISEARCH_GLOBALMATCHDATA_H
#define ISEARCH_GLOBALMATCHDATA_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(rank);

class GlobalMatchData
{
public:
    GlobalMatchData(int32_t docCount = 0);
    ~GlobalMatchData();
public:
    int32_t getDocCount() const ;
    void setDocCount(int32_t docCount);
private:
    int32_t _docCount;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_GLOBALMATCHDATA_H
