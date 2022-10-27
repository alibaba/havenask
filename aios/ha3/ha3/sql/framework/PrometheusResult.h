#pragma once

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <khronos_table_interface/CommonDefine.h>

BEGIN_HA3_NAMESPACE(sql);

class PrometheusResult : public autil::legacy::Jsonizable
{
public:
    PrometheusResult()
        : summary(-1.0)
        , messageWatermark(-1.0)
        , integrity(-1.0) {
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        json.Jsonize("metric", metric);
        json.Jsonize("summary", summary);
        json.Jsonize("messageWatermark", messageWatermark);
        json.Jsonize("integrity", integrity);
        json.Jsonize("values", values);
    }
public:
    double summary;
    khronos::KHR_WM_TYPE messageWatermark;
    float integrity;
    std::map<std::string, std::string> metric;
    std::vector<std::vector<autil::legacy::Any> > values;
};

typedef std::vector<PrometheusResult> PrometheusResults;

HA3_TYPEDEF_PTR(PrometheusResult);

END_HA3_NAMESPACE(sql);
