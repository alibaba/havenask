#include <ha3/common/SortDescription.h>

using namespace std;
using namespace suez::turing;

AUTIL_BEGIN_NAMESPACE(autil);
DECLARE_SIMPLE_TYPE(HA3_NS(common)::SortDescription::RankSortType);
AUTIL_END_NAMESPACE(autil);

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, SortDescription);

SortDescription::SortDescription() {
    _originalString = "";
    _sortAscendFlag = false;
    _type = RS_RANK;
    _rootSyntaxExpr = NULL;
}

SortDescription::SortDescription(const std::string &originalString) 
{
    _originalString = originalString;
    _sortAscendFlag = false;
    _type = RS_RANK;
    _rootSyntaxExpr = NULL;
}

SortDescription::~SortDescription() {
    delete _rootSyntaxExpr;
    _rootSyntaxExpr = NULL;
}

const string& SortDescription::getOriginalString() const {
    return _originalString;
}

void SortDescription::setOriginalString(const std::string &originalString) {
    _originalString = originalString;
}

bool SortDescription::getSortAscendFlag() const {
    return _sortAscendFlag;
}

void SortDescription::setSortAscendFlag(bool sortAscendFlag) {
    _sortAscendFlag = sortAscendFlag;
}

SyntaxExpr* SortDescription::getRootSyntaxExpr() const {
    return _rootSyntaxExpr;
}

void SortDescription::setRootSyntaxExpr(SyntaxExpr* expr) {
    if (NULL != _rootSyntaxExpr) {
        DELETE_AND_SET_NULL(_rootSyntaxExpr);
    }
    _rootSyntaxExpr = expr;
}

void SortDescription::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_rootSyntaxExpr);
    dataBuffer.write(_originalString);
    dataBuffer.write(_sortAscendFlag);
    dataBuffer.write(_type);
}

void SortDescription::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_rootSyntaxExpr);
    dataBuffer.read(_originalString);
    dataBuffer.read(_sortAscendFlag);
    dataBuffer.read(_type);
}

string SortDescription::toString() const 
{
    assert(_rootSyntaxExpr);
    if (_sortAscendFlag) {
        return "+(" + _rootSyntaxExpr->getExprString() + ")";
    } else {
        return "-(" + _rootSyntaxExpr->getExprString() + ")";
    }
}

END_HA3_NAMESPACE(common);

