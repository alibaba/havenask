#ifndef ISEARCH_TSDBRESULT_H
#define ISEARCH_TSDBRESULT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <khronos_table_interface/CommonDefine.h>

BEGIN_HA3_NAMESPACE(sql);

class TsdbResult : public autil::legacy::Jsonizable
{
public:
    TsdbResult()
        : summary(-1.0)
        , messageWatermark(-1.0)
        , integrity(-1.0) {
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        json.Jsonize("metric", metric);
        json.Jsonize("summary", summary);
        json.Jsonize("messageWatermark", messageWatermark);
        json.Jsonize("integrity", integrity);
        json.Jsonize("tags", tags);
        json.Jsonize("dps", dps);
    }
public:
    std::string metric;
    double summary;
    khronos::KHR_WM_TYPE messageWatermark;
    float integrity;
    std::map<std::string, std::string> tags;
    std::map<std::string, float> dps;
};

typedef std::vector<TsdbResult> TsdbResults;

HA3_TYPEDEF_PTR(TsdbResult);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_KHRONOSJSON_H
