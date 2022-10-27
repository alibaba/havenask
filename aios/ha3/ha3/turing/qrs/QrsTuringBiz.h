#ifndef ISEARCH_TURING_QRSTURINGBIZ_H
#define ISEARCH_TURING_QRSTURINGBIZ_H

#include <ha3/common.h>
#include <ha3/config/TypeDefine.h>
#include <suez/turing/search/Biz.h>

BEGIN_HA3_NAMESPACE(turing);

class QrsTuringBiz : public suez::turing::Biz
{
protected:
    std::string getBizInfoFile() override;
};

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_TURING_QRSTURINGBIZ_H
