#ifndef ISEARCH_AGGFUNCCREATOR_H
#define ISEARCH_AGGFUNCCREATOR_H

#include <ha3/sql/ops/agg/AggFunc.h>

BEGIN_HA3_NAMESPACE(sql);

class AggFuncCreator {
public:
    virtual ~AggFuncCreator() {}
public:
    AggFunc *createFunction(const std::vector<ValueType> &inputTypes,
                            const std::vector<std::string> &inputFields,
                            const std::vector<std::string> &outputFields,
                            bool isLocal)
    {
        assert(inputTypes.size() == inputFields.size());
        if (isLocal) {
            return createLocalFunction(inputTypes, inputFields, outputFields);
        } else {
            return createGlobalFunction(inputTypes, inputFields, outputFields);
        }
    }
private:
    virtual AggFunc *createLocalFunction(const std::vector<ValueType> &inputTypes,
            const std::vector<std::string> &inputFields,
            const std::vector<std::string> &outputFields) = 0;
    virtual AggFunc *createGlobalFunction(const std::vector<ValueType> &inputTypes,
            const std::vector<std::string> &inputFields,
            const std::vector<std::string> &outputFields) {
        return nullptr;
    }
};

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_AGGFUNCCREATOR_H
