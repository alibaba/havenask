#include <ha3/turing/ops/RequestUtil.h>
#include <ha3/common/ConfigClause.h>
#include <ha3/common/FinalSortClause.h>
#include <autil/StringTokenizer.h>
#include <ha3/queryparser/RequestSymbolDefine.h>

using namespace std;

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, RequestUtil);
USE_HA3_NAMESPACE(common);

const static string SORT_KEY = "sort_key";

RequestUtil::RequestUtil() { 
}

RequestUtil::~RequestUtil() { 
}

bool RequestUtil::defaultSorterBeginRequest(const common::RequestPtr &request) {
    std::string sorterName = "DEFAULT";
    auto finalSortClause = request->getFinalSortClause();
    if (finalSortClause) {
        sorterName = finalSortClause->getSortName();
    }
    if (sorterName != "DEFAULT") {
        return true;
    }

    if (!validateSortInfo(request)) {
        HA3_LOG(WARN, "validate sort info failed!");
        return false;
    }

    if (needScoreInSort(request)) {
        ConfigClause *configClause = request->getConfigClause();
        configClause->setNeedRerank(true);
    }
    return true;
}

bool RequestUtil::needScoreInSort(const common::RequestPtr &request) {
    FinalSortClause *finalSortClause = request->getFinalSortClause();
    if (finalSortClause) {
        const KeyValueMap &kvMap = finalSortClause->getParams();
        KeyValueMap::const_iterator iter = kvMap.find(SORT_KEY);
        if (iter != kvMap.end()) {
            string sortKeyDesc = iter->second;
            autil::StringTokenizer sortFields(sortKeyDesc, "|",
                    autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
            uint32_t fieldNum = sortFields.getNumTokens();
            for (uint32_t i  = 0; i < fieldNum; i++) {
                string sortField = sortFields[i];
                if (sortField[0] == SORT_CLAUSE_ASCEND) {
                    sortField = sortField.substr(1);
                } else if (sortField[0] == SORT_CLAUSE_DESCEND) {
                    sortField = sortField.substr(1);
                }
                if (sortField == SORT_CLAUSE_RANK) {
                    return true;
                }
            }
            return false;
        }
    }

    SortClause *sortClause = request->getSortClause();
    if (sortClause) {
        auto sortDescs = sortClause->getSortDescriptions(); 
        for (size_t i = 0; i < sortDescs.size(); i++) {
            if (sortDescs[i]->isRankExpression()) {
                return true;
            }
        }
        return false;
    }

    RankSortClause *rankSortClause = request->getRankSortClause();
    if (rankSortClause) {
        assert (rankSortClause->getRankSortDescCount() == 1);
        auto sortDescs = rankSortClause->getRankSortDesc(0)->getSortDescriptions();
        for (size_t i = 0; i < sortDescs.size(); i++) {
            if (sortDescs[i]->isRankExpression()) {
                return true;
            }
        }
        return false;
    }
  
    return true;
}

bool RequestUtil::validateSortInfo(const common::RequestPtr &request) {
    FinalSortClause *finalSortClause = request->getFinalSortClause();
    RankSortClause *rankSortClause = request->getRankSortClause();
    SortClause *sortClause = request->getSortClause();
    if (finalSortClause) {
        const KeyValueMap &kvMap = finalSortClause->getParams();
        KeyValueMap::const_iterator iter = kvMap.find(SORT_KEY);
        if (iter != kvMap.end()) {
            return true;
        }
    }
    if (!sortClause && rankSortClause) {
        if (rankSortClause->getRankSortDescCount() > 1) {
            string errorMsg = "use multi dimension rank_sort clause,"
                              " without specify sort_key in final_sort clause!";
            HA3_LOG(WARN, "%s", errorMsg.c_str());
            return false;
        }
    }
    return true;
}

END_HA3_NAMESPACE(turing);
