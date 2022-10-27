#ifndef ISEARCH_LAYERCLAUSEVALIDATOR_H
#define ISEARCH_LAYERCLAUSEVALIDATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/AttributeInfos.h>
#include <ha3/common/ErrorDefine.h>
#include <ha3/common/QueryLayerClause.h>
BEGIN_HA3_NAMESPACE(qrs);

class LayerClauseValidator
{
public:
    LayerClauseValidator(const suez::turing::AttributeInfos *attrInfos);
    ~LayerClauseValidator();
private:
    LayerClauseValidator(const LayerClauseValidator &);
    LayerClauseValidator& operator=(const LayerClauseValidator &);
public:
    bool validate(common::QueryLayerClause* queryLayerClause);
    ErrorCode getErrorCode() const;
    const std::string &getErrorMsg() const;
private:
    bool isRangeKeyWord(const std::string &keyName);
    bool checkRange(const common::QueryLayerClause *layerClause);
    bool checkField(const common::QueryLayerClause* layerClause);
    bool checkSequence(const common::QueryLayerClause* layerClause);
public:
    // public for test
    void autoAddSortedRange(common::QueryLayerClause* layerClause);
private:
    const suez::turing::AttributeInfos *_attrInfos;
    ErrorCode _errorCode;
    std::string _errorMsg;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LayerClauseValidator);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_LAYERCLAUSEVALIDATOR_H
