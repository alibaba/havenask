#ifndef __INDEXLIB_SIMPLE_TABLE_READER_H
#define __INDEXLIB_SIMPLE_TABLE_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/table_reader.h"
#include "indexlib/test/result.h"

namespace indexlib { namespace test {

class SimpleTableReader : public table::TableReader
{
public:
    SimpleTableReader() {}
    ~SimpleTableReader() {}

public:
    virtual ResultPtr Search(const std::string& query) const = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SimpleTableReader);
}} // namespace indexlib::test

#endif //__INDEXLIB_SIMPLE_TABLE_READER_H
