#include <ha3/common/DistinctDescription.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, DistinctDescription);

static string BoolToString(bool flag) {
    if (flag) {
        return string("true");
    } else {
        return string("false");
    }
}

DistinctDescription::DistinctDescription(const string &moduleName,
        const string &originalString, int32_t distinctCount, int32_t distinctTimes,
        int32_t maxItemCount, bool reservedFlag, bool updateTotalHitFlag,
        SyntaxExpr *syntaxExpr, FilterClause *filterClause)
    : _moduleName(moduleName)
    , _originalString(originalString)
    , _distinctTimes(distinctTimes)
    , _distinctCount(distinctCount)
    , _maxItemCount(maxItemCount)
    , _reservedFlag(reservedFlag)
    , _updateTotalHitFlag(updateTotalHitFlag)
    , _rootSyntaxExpr(syntaxExpr)
    , _filterClause(filterClause)
{
}

DistinctDescription::~DistinctDescription() {
    delete _rootSyntaxExpr;
    _rootSyntaxExpr = NULL;
    if (_filterClause) {
        delete _filterClause;
        _filterClause = NULL;
    }
}

int32_t DistinctDescription::getDistinctCount() const {
    return _distinctCount;
}

void DistinctDescription::setDistinctCount(int32_t distinctCount) {
    _distinctCount = distinctCount;
}

int32_t DistinctDescription::getDistinctTimes() const {
    return _distinctTimes;
}

void DistinctDescription::setDistinctTimes(int32_t distinctTimes) {
    _distinctTimes = distinctTimes;
}

int32_t DistinctDescription::getMaxItemCount() const {
    return _maxItemCount;
}

void DistinctDescription::setMaxItemCount(int32_t maxItemCount) {
    _maxItemCount = maxItemCount;
}

bool DistinctDescription::getReservedFlag() const {
    return _reservedFlag;
}

void DistinctDescription::setReservedFlag(bool reservedFalg) {
    _reservedFlag = reservedFalg;
}

bool DistinctDescription::getUpdateTotalHitFlag() const {
    return _updateTotalHitFlag;
}
void DistinctDescription::setUpdateTotalHitFlag(bool updateTotalHitFlag) {
    _updateTotalHitFlag = updateTotalHitFlag;
}

void DistinctDescription::setRootSyntaxExpr(SyntaxExpr* syntaxExpr){
    _rootSyntaxExpr = syntaxExpr;
}

SyntaxExpr *DistinctDescription::getRootSyntaxExpr() const
{
    return _rootSyntaxExpr;
}

void DistinctDescription::setGradeThresholds(const vector<double>& gradeThresholds) {
    _gradeThresholds = gradeThresholds;
}
const vector<double>& DistinctDescription::getGradeThresholds() const 
{
    return _gradeThresholds;
}

void DistinctDescription::setOriginalString(const std::string& originalString) {
    _originalString = originalString;
}

const std::string& DistinctDescription::getOriginalString() const {
    return _originalString;
}

void DistinctDescription::setModuleName(const std::string& moduleName) {
    _moduleName = moduleName;
}

const std::string &DistinctDescription::getModuleName() const {
    return _moduleName;
}

void DistinctDescription::setFilterClause(FilterClause* filterClause) {
    if(_filterClause) {
        delete _filterClause;
        _filterClause = NULL;
    }
    _filterClause = filterClause;
}

FilterClause* DistinctDescription::getFilterClause() const {
    return _filterClause;
}

void DistinctDescription::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_moduleName);
    dataBuffer.write(_distinctTimes);
    dataBuffer.write(_distinctCount);
    dataBuffer.write(_maxItemCount);
    dataBuffer.write(_reservedFlag);
    dataBuffer.write(_updateTotalHitFlag);
    dataBuffer.write(_originalString);
    dataBuffer.write(_rootSyntaxExpr);
    dataBuffer.write(_filterClause);
    dataBuffer.write(_gradeThresholds);
}

void DistinctDescription::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_moduleName);
    dataBuffer.read(_distinctTimes);
    dataBuffer.read(_distinctCount);
    dataBuffer.read(_maxItemCount);
    dataBuffer.read(_reservedFlag);
    dataBuffer.read(_updateTotalHitFlag);
    dataBuffer.read(_originalString);
    dataBuffer.read(_rootSyntaxExpr);
    dataBuffer.read(_filterClause);
    dataBuffer.read(_gradeThresholds);
}

std::string DistinctDescription::toString() const {
    assert(_rootSyntaxExpr);
    std::string distDescStr;
    distDescStr.append("moduleName:");
    distDescStr.append(_moduleName);
    distDescStr.append(",");
    distDescStr.append("distkey:");
    distDescStr.append(_rootSyntaxExpr->getExprString());
    distDescStr.append(",");
    distDescStr.append("distincttimes:");
    distDescStr.append(StringUtil::toString(_distinctTimes));
    distDescStr.append(",");
    distDescStr.append("distinctcount:");
    distDescStr.append(StringUtil::toString(_distinctCount));
    distDescStr.append(",");
    distDescStr.append("maxitemcount:");
    distDescStr.append(StringUtil::toString(_maxItemCount));
    distDescStr.append(",");
    distDescStr.append("reservedflag:");
    distDescStr.append(BoolToString(_reservedFlag));
    distDescStr.append(",");
    distDescStr.append("updatetotalhitflag:");
    distDescStr.append(BoolToString(_updateTotalHitFlag));
    if (_filterClause) {
        distDescStr.append(",");
        distDescStr.append("filter:");
        distDescStr.append(_filterClause->toString());
    }
    distDescStr.append(",");
    distDescStr.append("grades:");
    for (size_t i = 0; i < _gradeThresholds.size(); i++) {
        distDescStr.append(StringUtil::toString(_gradeThresholds[i]));
        distDescStr.append("|");
    }
    return distDescStr;
}

END_HA3_NAMESPACE(common);
