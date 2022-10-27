#ifndef ISEARCH_TABLEQUERY_H
#define ISEARCH_TABLEQUERY_H

#include <ha3/common/Query.h>
#include <ha3/common/ColumnTerm.h>

BEGIN_HA3_NAMESPACE(common);

class TableQuery : public Query
{
public:
    using TermArray = std::vector<ColumnTerm*>;
public:
    TableQuery(
        const std::string &label,
        QueryOperator rowOp = OP_OR,
        QueryOperator columnOp = OP_AND,
        QueryOperator multiValueOp = OP_OR);
    ~TableQuery();
private:
    TableQuery(const TableQuery &) = default;
public:
    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;

    std::string getQueryName() const override {
        return "TableQuery";
    }
    std::string toString() const override;
    const TermArray& getTermArray() const {
        return _terms;
    }
    TermArray& getTermArray() {
        return _terms;
    }
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;

    QueryType getType() const override {
        return TABLE_QUERY;
    }
    QueryOperator getRowOp() const {
        return _rowOp;
    }
    QueryOperator getColumnOp() const {
        return _columnOp;
    }
    QueryOperator getMultiValueOp() const {
        return _multiValueOp;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelTerm(label);
    }
private:
    TermArray _terms;
    QueryOperator _rowOp;
    QueryOperator _columnOp;
    QueryOperator _multiValueOp;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TableQuery);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_TABLEQUERY_H
