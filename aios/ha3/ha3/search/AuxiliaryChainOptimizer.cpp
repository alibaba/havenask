#include <ha3/search/AuxiliaryChainOptimizer.h>
#include <ha3/common/QueryTermVisitor.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/AuxiliaryChainVisitor.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, AuxiliaryChainOptimizer);

AuxiliaryChainOptimizer::AuxiliaryChainOptimizer() { 
    _queryInsertType = QI_OVERWRITE;
    _selectAuxChainType = SAC_DF_SMALLER;
    _active = true;
}

AuxiliaryChainOptimizer::~AuxiliaryChainOptimizer() { 
}

OptimizerPtr AuxiliaryChainOptimizer::clone() {
    return OptimizerPtr(new AuxiliaryChainOptimizer(*this));
}

bool AuxiliaryChainOptimizer::init(OptimizeInitParam *param) {
    const string &option = param->optimizeOption;
    StringTokenizer st(option, "|", 
                       StringTokenizer::TOKEN_IGNORE_EMPTY |
                       StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer st1(st[i], "#", 
                            StringTokenizer::TOKEN_IGNORE_EMPTY |
                            StringTokenizer::TOKEN_TRIM);
        if (st1.getNumTokens() != 2) {
            HA3_LOG(WARN, "Invalid option[%s] for AuxiliaryChainOptimizer", st[i].c_str());
            return false;
        }
        if (st1[0] == "select") {
            SelectAuxChainType type = covertSelectAuxChainType(st1[1]);
            if (type == SAC_INVALID) {
                HA3_LOG(WARN, "Invalid select[%s] type for AuxiliaryChainOptimizer", st1[1].c_str());
                return false;
            }
            _selectAuxChainType = type;
        } else if (st1[0] == "pos") {
            QueryInsertType type = covertQueryInsertType(st1[1]);
            if (type == QI_INVALID) {
                HA3_LOG(WARN, "Invalid pos[%s] type for AuxiliaryChainOptimizer", st1[1].c_str());
                return false;
            }
            _queryInsertType = type;
        } else if (st1[0] == "aux_name") {
            _auxChainName = st1[1];
        } else {
            HA3_LOG(WARN, "Invalid option[%s] for AuxiliaryChainOptimizer", st[i].c_str());
            return false;
        }
    }
    return true;
}

void AuxiliaryChainOptimizer::disableTruncate() {
    _active = _auxChainName == BITMAP_AUX_NAME;
}

bool AuxiliaryChainOptimizer::optimize(OptimizeParam *param)
{
    if (!_active) {
        return true;
    }
    const Request *request = param->request;
    IndexPartitionReaderWrapper *readerWrapper = param->readerWrapper;
    QueryClause *queryClause = request->getQueryClause();
    assert(queryClause->getQueryCount() >= 1);
    Query *query = queryClause->getRootQuery(0);

    QueryTermVisitor visitor(QueryTermVisitor::VT_ANDNOT_QUERY);
    query->accept(&visitor);
    const TermVector& termVector = visitor.getTermVector();
    TermDFMap termDFMap;
    if (_selectAuxChainType != SAC_ALL) {
        getTermDFs(termVector, readerWrapper, termDFMap);
    }
    insertNewQuery(termDFMap, queryClause);

    return true;
}

void AuxiliaryChainOptimizer::getTermDFs(const TermVector &termVector,
        IndexPartitionReaderWrapper *readerWrapper, TermDFMap &termDFMap)
{
    for (TermVector::const_iterator it = termVector.begin();
         it != termVector.end(); ++it)
    {
        termDFMap[*it] = readerWrapper->getTermDF(*it);
    }
}

void AuxiliaryChainOptimizer::insertNewQuery(
        const TermDFMap &termDFMap, QueryClause *queryClause)
{
    Query *query = queryClause->getRootQuery(0);
    assert(query);
    if (_queryInsertType != QI_OVERWRITE) {
        query = query->clone();
    }
    AuxiliaryChainVisitor visitor(_auxChainName, termDFMap, _selectAuxChainType);
    query->accept(&visitor);

    if (_queryInsertType == QI_BEFORE) {
        queryClause->insertQuery(query, 0);
    } else if (_queryInsertType == QI_AFTER) {
        queryClause->insertQuery(query, 1);
    }
}

QueryInsertType AuxiliaryChainOptimizer::covertQueryInsertType(const std::string &str) {
    if (str == "BEFORE" || str == "before") {
        return QI_BEFORE;
    } else if (str == "AFTER" || str == "after") {
        return QI_AFTER;
    } else if (str == "OVERWRITE" || str == "overwrite") {
        return QI_OVERWRITE;
    }
    return QI_INVALID;
}

SelectAuxChainType AuxiliaryChainOptimizer::covertSelectAuxChainType(const std::string &str) {
    if (str == "BIGGER" || str == "bigger") {
        return SAC_DF_BIGGER;
    } else if (str == "SMALLER" || str == "smaller") {
        return SAC_DF_SMALLER;
    } else if (str == "ALL" || str == "all") {
        return SAC_ALL;
    }
    return SAC_INVALID;
}

END_HA3_NAMESPACE(search);

