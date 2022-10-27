#include "ha3/sql/ops/tvf/TvfFunc.h"

using namespace std;
BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, TvfFunc);

bool OnePassTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    if (_table == nullptr) {
        _table = input;
    } else {
        if (input != nullptr && !_table->merge(input)) {
            SQL_LOG(ERROR, "tvf [%s] merge input table failed", getName().c_str());
            return false;
        }
    }
    if (eof) {
        if (_table == nullptr) {
            SQL_LOG(ERROR, "tvf [%s] input is null", getName().c_str());
            return false;
        }
        bool ret = doCompute(_table, output);
        if (output == nullptr) {
            SQL_LOG(ERROR, "tvf [%s] output is null", getName().c_str());
            return false;
        }
        _table.reset();
        return ret;
    }
    return true;
}


END_HA3_NAMESPACE(sql)
