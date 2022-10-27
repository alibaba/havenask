#ifndef ISEARCH_HASHJOININFO_H
#define ISEARCH_HASHJOININFO_H

#include <string>
#include <unordered_map>

#include <ha3/isearch.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/framework/JoinDocIdConverterBase.h>

BEGIN_HA3_NAMESPACE(common);

class HashJoinInfo
{
public:
    typedef std::unordered_map<size_t, std::vector<int32_t>> HashJoinMap;
    using DocIdWrapper = suez::turing::DocIdWrapper;
    HashJoinInfo(const std::string &auxTableName,
                 const std::string &joinFieldName)
        : _auxTableName(auxTableName)
        , _joinFieldName(joinFieldName)
        , _joinAttrExpr(nullptr)
        , _auxDocidRef(nullptr) {}

    ~HashJoinInfo() {}

private:
    HashJoinInfo(const HashJoinInfo &);
    HashJoinInfo& operator=(const HashJoinInfo &);
public:
    void setAuxTableName(const std::string &auxTableName) {
	_auxTableName = auxTableName;
    }
    const std::string &getAuxTableName() const {
	return _auxTableName;
    }
    void setJoinFieldName(const std::string &joinFieldName) {
        _joinFieldName = joinFieldName;
    }
    const std::string &getJoinFieldName() const {
	return _joinFieldName;
    }
    void setJoinAttrExpr(suez::turing::AttributeExpression *joinAttrExpr) {
        _joinAttrExpr = joinAttrExpr;
    }
    suez::turing::AttributeExpression *getJoinAttrExpr() {
	return _joinAttrExpr;
    }
    void setAuxDocidRef(matchdoc::Reference<DocIdWrapper> *ref) {
	_auxDocidRef = ref;
    }
    matchdoc::Reference<DocIdWrapper> *getAuxDocidRef() const {
        return _auxDocidRef;
    }
    const HashJoinMap &getHashJoinMap() const { return _hashJoinMap; }
    HashJoinMap &getHashJoinMap() { return _hashJoinMap; }

private:
    std::string _auxTableName;
    std::string _joinFieldName;
    suez::turing::AttributeExpression *_joinAttrExpr;
    matchdoc::Reference<DocIdWrapper> *_auxDocidRef;
    HashJoinMap _hashJoinMap;
private:
    HA3_LOG_DECLARE();
};


HA3_TYPEDEF_PTR(HashJoinInfo);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_HASHJOININFO_H
