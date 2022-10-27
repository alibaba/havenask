#ifndef ISEARCH_RANGEQUERYEXECUTOR_H
#define ISEARCH_RANGEQUERYEXECUTOR_H

#include <ha3/sql/common/common.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/LayerMetas.h>
#include <indexlib/partition/index_partition_reader.h>

BEGIN_HA3_NAMESPACE(sql);

class RangeQueryExecutor : public search::QueryExecutor
{
public:
    RangeQueryExecutor(search::LayerMeta* layerMeta);
    ~RangeQueryExecutor();
private:
    RangeQueryExecutor(const RangeQueryExecutor &);
    RangeQueryExecutor& operator=(const RangeQueryExecutor &);
public:
    const std::string getName() const override {
        return "RangeQueryExecutor";
    }
    df_t getDF(GetDFType type) const override {
        return _df;
    }
    void reset() override;
    IE_NAMESPACE(common)::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
            docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;
    bool isMainDocHit(docid_t docId) const override;
public:
    std::string toString() const override;
private:
    virtual IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;

private:
    df_t _df;
    search::LayerMeta& _layerMeta;
    size_t _rangeIdx;
    size_t _rangeCount;
};

HA3_TYPEDEF_PTR(RangeQueryExecutor);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_RANGEQUERYEXECUTOR_H
