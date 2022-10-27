#ifndef ISEARCH_MATCHDOCSEARCHERHELPER_H
#define ISEARCH_MATCHDOCSEARCHERHELPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(sorter);

class MatchDocSearcherHelper
{
public:
    MatchDocSearcherHelper();
    ~MatchDocSearcherHelper();
private:
    MatchDocSearcherHelper(const MatchDocSearcherHelper &);
    MatchDocSearcherHelper& operator=(const MatchDocSearcherHelper &);
public:
    static build_service::analyzer::AnalyzerFactoryPtr initFactory();
    static bool compareVariableReference(common::VariableReferenceBase* left,
            common::VariableReferenceBase* right);
    static common::RequestPtr prepareRequest(const std::string &query, 
            const config::TableInfoPtr &tableInfoPtr,
            const std::string &defaultIndexName = "phrase");
    static IndexPartitionReaderWrapperPtr makeIndexPartitionReaderWrapper(
            const index::IndexPartitionReaderPtr &indexPartReaderPtr);
private:
    static build_service::analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MatchDocSearcherHelper);

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_MATCHDOCSEARCHERHELPER_H
