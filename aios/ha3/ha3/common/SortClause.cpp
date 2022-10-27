#include <ha3/common/SortClause.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>

using namespace std;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, SortClause);

SortClause::SortClause() { 
}

SortClause::~SortClause() { 
    clearSortDescriptions();
}

const std::vector<SortDescription*>& SortClause::getSortDescriptions() const {
    return _sortDescriptions;
}

void SortClause::addSortDescription(SortDescription *sortDescription) {
    if (sortDescription) {
        HA3_LOG(DEBUG, "originalString: %s", sortDescription->getOriginalString().c_str());
        _sortDescriptions.push_back(sortDescription);
    }
}

void SortClause::clearSortDescriptions() {
    for (vector<SortDescription*>::iterator it = _sortDescriptions.begin();
         it != _sortDescriptions.end(); it++)
    {
        delete *it;
        *it = NULL;
    }
    _sortDescriptions.clear();
}

void SortClause::addSortDescription(SyntaxExpr* syntaxExpr,
                                    bool isRank,
                                    bool sortAscendFlag) 
{
    SortDescription *sortDescription = new SortDescription();
    sortDescription->setExpressionType(isRank ? SortDescription::RS_RANK : SortDescription::RS_NORMAL);
//    sortDescription->setOriginalString(originalString);
    sortDescription->setSortAscendFlag(sortAscendFlag);
    sortDescription->setRootSyntaxExpr(syntaxExpr);
    addSortDescription(sortDescription);
}

SortDescription* SortClause::getSortDescription(uint32_t pos) {
    if (pos >= _sortDescriptions.size()) {
        return NULL;
    }
    return _sortDescriptions[pos];
}

void SortClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_sortDescriptions);
    dataBuffer.write(_originalString);
}

void SortClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_sortDescriptions);
    dataBuffer.read(_originalString);
}

string SortClause::toString() const {
    string sortStr;
    size_t count = _sortDescriptions.size();
    for (size_t i = 0; i < count; i++) {
        sortStr.append(_sortDescriptions[i]->toString());
        sortStr.append(";");
    }
    return sortStr;
}

END_HA3_NAMESPACE(common);

