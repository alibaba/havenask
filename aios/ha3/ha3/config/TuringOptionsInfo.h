#ifndef ISEARCH_TURINGOPTIONSINFO_H
#define ISEARCH_TURINGOPTIONSINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

class TuringOptionsInfo : public autil::legacy::Jsonizable
{
public:
    TuringOptionsInfo()
    {
    }

    ~TuringOptionsInfo() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
public:
    autil::legacy::json::JsonMap _turingOptionsInfo;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TuringOptionsInfo);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_TURINGOPTIONSINFO_H
