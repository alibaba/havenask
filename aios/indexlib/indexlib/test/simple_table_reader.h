#ifndef __INDEXLIB_SIMPLE_TABLE_READER_H
#define __INDEXLIB_SIMPLE_TABLE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/table_reader.h"
#include "indexlib/test/query.h"
#include "indexlib/testlib/result.h"

IE_NAMESPACE_BEGIN(test);

class SimpleTableReader : public table::TableReader
{
public:
    SimpleTableReader() {}
    ~SimpleTableReader() {}
public:
    virtual testlib::ResultPtr Search(const std::string& query) const = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SimpleTableReader);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_SIMPLE_TABLE_READER_H
