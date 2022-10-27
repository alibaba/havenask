#include <ha3/sql/data/TableFormatter.h>
#include <ha3/sql/data/TableUtil.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);

TableFormatter::TableFormatter() {
}

TableFormatter::~TableFormatter() {
}

string TableFormatter::format(TablePtr &table, const std::string &type) {
    if (type == "json") {
        return TableUtil::toJsonString(table);
    } else {
        return TableUtil::toString(table);
    }
}

END_HA3_NAMESPACE(sql);
