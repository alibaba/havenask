#ifndef ISEARCH_SQL_LOGICAL_SCAN_H
#define ISEARCH_SQL_LOGICAL_SCAN_H

#include <ha3/sql/ops/scan/ScanBase.h>

BEGIN_HA3_NAMESPACE(sql);

class LogicalScan : public ScanBase {
public:
    LogicalScan();
    virtual ~LogicalScan();
private:
    LogicalScan(const LogicalScan&);
    LogicalScan& operator=(const LogicalScan &);
public:
    bool init(const ScanInitParam &param) override;
    bool doBatchScan(TablePtr &table, bool &eof) override;

private:
    TablePtr createTable();

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LogicalScan);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_LOGICAL_KERNEL_H