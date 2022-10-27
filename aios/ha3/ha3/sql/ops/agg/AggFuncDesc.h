#ifndef ISEARCH_AGGFUNCDESC_H
#define ISEARCH_AGGFUNCDESC_H

#include <autil/legacy/jsonizable.h>
#include <ha3/sql/ops/util/KernelUtil.h>

BEGIN_HA3_NAMESPACE(sql);

class AggFuncDesc : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        json.Jsonize("type", type);
        json.Jsonize("name", funcName);
        json.Jsonize("input", inputs, inputs);
        for (size_t i = 0; i < inputs.size(); ++i) {
            KernelUtil::stripName(inputs[i]);
        }
        json.Jsonize("output", outputs);
        for (size_t i = 0; i < outputs.size(); ++i) {
            KernelUtil::stripName(outputs[i]);
        }
        json.Jsonize("filter_arg", filterArg, -1);
    }
public:
    std::string type;
    std::string funcName;
    std::vector<std::string> inputs;
    std::vector<std::string> outputs;
    int32_t filterArg;
};

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_AGGFUNCDESC_H
