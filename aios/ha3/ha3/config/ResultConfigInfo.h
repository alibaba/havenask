#ifndef ISEARCH_RESULTCONFIGINFO_H
#define ISEARCH_RESULTCONFIGINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(config);

class ResultConfigInfo
{
public:
    ResultConfigInfo();
    ~ResultConfigInfo();
public:

private:
    HA3_LOG_DECLARE();
};

class ResultConfigInfos {

};

END_HA3_NAMESPACE(config);

#endif //ISEARCH_RESULTCONFIGINFO_H
