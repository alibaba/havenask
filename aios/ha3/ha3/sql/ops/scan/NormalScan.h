#ifndef ISEARCH_SQL_NORMAL_SCAN_H
#define ISEARCH_SQL_NORMAL_SCAN_H

#include <ha3/sql/ops/scan/ScanBase.h>
#include <matchdoc/MatchDocAllocator.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/framework/AttributeExpression.h>

BEGIN_HA3_NAMESPACE(sql);
class NormalScan : public ScanBase {
public:
    NormalScan();
    virtual ~NormalScan();
private:
    NormalScan(const NormalScan&);
    NormalScan& operator=(const NormalScan &);
public:
    bool init(const ScanInitParam &param) override;
    bool doBatchScan(TablePtr &table, bool &eof) override;
    bool updateScanQuery(const StreamQueryPtr &inputQuery) override;

private:
    bool initOutputColumn();
    bool copyField(const std::string &expr, const std::string &outputName, 
                   std::map<std::string, std::pair<std::string, bool> > &expr2Outputs);
    suez::turing::AttributeExpression*
    initAttributeExpr(std::string outputName,std::string outputFieldType,std::string exprStr,
                      std::map<std::string, std::pair<std::string, bool> >& expr2Outputs);
    void evaluateAttribute(std::vector<matchdoc::MatchDoc> &matchDocVec,
                           matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                           std::vector<suez::turing::AttributeExpression *> &attributeExpressionVec,
                           bool eof);
    TablePtr createTable(std::vector<matchdoc::MatchDoc> &matchDocVec,
                         const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                         bool reuseMatchDocAllocator);
    void flattenSub(matchdoc::MatchDocAllocatorPtr &outputAllocator,
                           const std::vector<matchdoc::MatchDoc> &copyMatchDocs,
                           TablePtr &table);
    bool appendCopyColumn(const std::string &srcField, const std::string &destField,
                          matchdoc::MatchDocAllocatorPtr &matchDocAllocator);
    void copyColumns(const std::map<std::string, std::string> &copyFieldMap,
                     const std::vector<matchdoc::MatchDoc> &matchDocVec,
                     matchdoc::MatchDocAllocatorPtr &matchDocAllocator);

private:
    std::map<std::string, std::string> _copyFieldMap;
    std::vector<suez::turing::AttributeExpression *> _attributeExpressionVec;
    ScanIteratorPtr _scanIter;
    std::string _tableMeta;
    ScanIteratorCreatorPtr _scanIterCreator;
    CreateScanIteratorInfo _baseCreateScanIterInfo;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(NormalScan);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_SCAN_KERNEL_H
