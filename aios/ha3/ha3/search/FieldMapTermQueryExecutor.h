#ifndef ISEARCH_FIELDMAPTERMQUERYEXECUTOR_H
#define ISEARCH_FIELDMAPTERMQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/BufferedTermQueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

enum FieldMatchOperatorType {
    FM_AND,
    FM_OR
};

class FieldMapTermQueryExecutor : public BufferedTermQueryExecutor
{
public:
    FieldMapTermQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter, 
                              const common::Term &term,
                              fieldmap_t fieldMap,
                              FieldMatchOperatorType opteratorType);
    ~FieldMapTermQueryExecutor();
private:
    FieldMapTermQueryExecutor(const FieldMapTermQueryExecutor &);
    FieldMapTermQueryExecutor& operator=(const FieldMapTermQueryExecutor &);
public:
    std::string toString() const override;
private:
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;
private:
    fieldmap_t _fieldMap;
    FieldMatchOperatorType _opteratorType;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FieldMapTermQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FIELDMAPTERMQUERYEXECUTOR_H
