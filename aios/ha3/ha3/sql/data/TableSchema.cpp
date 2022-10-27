#include <ha3/sql/data/TableSchema.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, TableSchema);

bool TableSchema::addColumnSchema(const ColumnSchemaPtr &columnSchema) {
    const string &name = columnSchema->getName();
    if (_columnSchema.find(name) != _columnSchema.end()) {
        SQL_LOG(WARN, "column name [%s] already exists.", name.c_str());
        return false;
    }
    _columnSchema.insert(pair<string, ColumnSchemaPtr>(name, columnSchema));
    return true;
}

bool TableSchema::eraseColumnSchema(const std::string &name) {
    auto iter = _columnSchema.find(name);
    if (iter == _columnSchema.end()) {
        SQL_LOG(WARN, "column [%s] does not exist.", name.c_str());
        return false;
    }
    _columnSchema.erase(iter);
    return true;
}

ColumnSchema* TableSchema::getColumnSchema(const string &name) const {
    auto iter = _columnSchema.find(name);
    if (iter == _columnSchema.end()) {
        SQL_LOG(WARN, "column [%s] does not exist.", name.c_str());
        return NULL;
    }
    return iter->second.get();
}

std::string TableSchema::toString() const{
    string str;
    for (auto iter :_columnSchema) {
        str += iter.second->toString() + ";";
    }
    return str;
}

bool TableSchema::operator == (const TableSchema& other) const {
    if (_columnSchema.size() != other._columnSchema.size()) {
        return false;
    }
    auto iter1 = _columnSchema.begin();
    auto iter2 = other._columnSchema.begin();
    while (iter1 != _columnSchema.end() && iter2 != other._columnSchema.end()) {
        if (iter1->first != iter2->first || *(iter1->second) != *(iter2->second)) {
            return false;
        }
        iter1++;
        iter2++;
    }
    return true;
}

END_HA3_NAMESPACE(sql);
