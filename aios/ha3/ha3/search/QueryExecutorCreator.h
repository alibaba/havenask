#ifndef ISEARCH_QUERYEXECUTORCREATOR_H
#define ISEARCH_QUERYEXECUTORCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/common/term.h>
#include <indexlib/common/number_term.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <indexlib/index/normal/primarykey/primary_key_index_reader_typed.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/MultiQueryExecutor.h>
#include <ha3/search/MatchDataManager.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/search/LayerMetas.h>

BEGIN_HA3_NAMESPACE(search);

class AndQueryExecutor;
class TermQueryExecutor;
class LiteTerm;
class QueryExecutorCreator : public common::QueryVisitor
{
public:
    QueryExecutorCreator(MatchDataManager *matchDataManager,
                         IndexPartitionReaderWrapper *readerWrapper,
                         autil::mem_pool::Pool *pool,
                         common::TimeoutTerminator *timer = NULL,
                         const LayerMeta *layerMeta = NULL);
    virtual ~QueryExecutorCreator();
public:
    void visitTermQuery(const common::TermQuery *query);
    void visitPhraseQuery(const common::PhraseQuery *query);
    void visitAndQuery(const common::AndQuery *query);
    void visitOrQuery(const common::OrQuery *query);
    void visitAndNotQuery(const common::AndNotQuery *query);
    void visitRankQuery(const common::RankQuery *query);
    void visitNumberQuery(const common::NumberQuery *query);
    void visitMultiTermQuery(const common::MultiTermQuery *query);
    void visitTableQuery(const common::TableQuery *query);

    QueryExecutor* stealQuery();
private:
    static std::string composeTruncateName(const std::string &word,
            const std::string &chainName)
    {
        std::string truncateName = word;
        truncateName += DYNAMIC_CUT_SEPARATOR + chainName;
        return truncateName;
    }

    bool canConvertToBitmapAndQuery(const std::vector<QueryExecutor*> &queryExecutors);
    MultiQueryExecutor *createAndQueryExecutor(const std::vector<QueryExecutor*> &queryExecutors);
    MultiQueryExecutor *createMultiTermAndQueryExecutor(const std::vector<QueryExecutor*> &queryExecutors);
    void addUnpackQuery(TermQueryExecutor *termQuery, const common::Term &term, const common::Query *query);
    void addUnpackQuery(QueryExecutor *queryExecutor, const common::Query *query);
    std::vector<QueryExecutor *> colTermProcess(
            const common::ColumnTerm* col,
            const std::vector<QueryExecutor *> &termExecutors,
            QueryOperator op);
    QueryExecutor *mergeQueryExecutor(
            const std::vector<QueryExecutor *> &childQueryExecutors,
            QueryOperator op);
    void visitColumnTerm(
        const common::ColumnTerm &term,
        std::vector<LookupResult> &results);
    void visitNumberColumnTerm(
        const IE_NAMESPACE(index)::IndexReaderPtr &indexReader,
        bool isSubIndex,
        const common::ColumnTerm &term,
        std::vector<LookupResult> &results);
    void visitPk64ColumnTerm(
        const IE_NAMESPACE(index)::IndexReaderPtr &indexReader,
        bool isSubIndex,
        const common::ColumnTerm &term,
        std::vector<LookupResult> &results);
    void visitNormalColumnTerm(
        const IE_NAMESPACE(index)::IndexReaderPtr &indexReader,
        bool isSubIndex,
        const common::ColumnTerm &term,
        std::vector<LookupResult> &results);
    template <class T, class Traits>
    LookupResult lookupNormalIndexWithoutCache(
        Traits traits,
        const IE_NAMESPACE(index)::IndexReaderPtr &indexReader,
        bool isSubIndex,
        LiteTerm &term,
        PostingType pt1,
        PostingType pt2,
        const T &value);
    template <class T, class Traits>
    void lookupNormalIndex(
        Traits traits,
        const IE_NAMESPACE(index)::IndexReaderPtr &indexReader,
        bool isSubIndex,
        const common::ColumnTerm &term,
        std::vector<LookupResult> &results);
    template <class T, class Traits>
    LookupResult lookupPk64IndexWithoutCache(
        Traits traits,
        const IE_NAMESPACE(index)::UInt64PrimaryKeyIndexReaderPtr &pkReader,
        bool isSubIndex,
        const T &value);
    template <class T, class Traits>
    void lookupPk64Index(
        Traits traits,
        const IE_NAMESPACE(index)::IndexReaderPtr &indexReader,
        bool isSubIndex,
        const common::ColumnTerm &term,
        std::vector<LookupResult> &results);
public:
    static TermQueryExecutor *createTermQueryExecutor(
            IndexPartitionReaderWrapper *readerWrapper,
            const common::Term &queryTerm,
            autil::mem_pool::Pool *pool,
            const LayerMeta *layerMeta);
    static TermQueryExecutor *createTermQueryExecutor(
            IndexPartitionReaderWrapper *readerWrapper,
            const common::Term &queryTerm,
            const LookupResult &result,
            autil::mem_pool::Pool *pool,
            const LayerMeta *layerMeta);
private:
    static TermQueryExecutor *doCreateTermQueryExecutor(
            IndexPartitionReaderWrapper *readerWrapper,
            const common::Term &queryTerm,
            const LookupResult &result,
            autil::mem_pool::Pool *pool,
            const LayerMeta *layerMeta);
private:
    MatchDataManager *_matchDataManager;
    QueryExecutor *_queryExecutor;
    IndexPartitionReaderWrapper *_readerWrapper;
    int32_t _andnotQueryLevel; //add left termQueryexecutor
    autil::mem_pool::Pool *_pool;
    common::TimeoutTerminator *_timer;
    const LayerMeta *_layerMeta;
private:
    friend class QueryExecutorCreatorTest;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_QUERYEXECUTORCREATOR_H
