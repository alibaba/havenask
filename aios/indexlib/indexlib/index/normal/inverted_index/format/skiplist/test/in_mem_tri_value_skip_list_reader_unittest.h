#ifndef __INDEXLIB_INMEMTRIVALUESKIPLISTREADERTEST_H
#define __INDEXLIB_INMEMTRIVALUESKIPLISTREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/in_mem_tri_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/test/tri_value_skip_list_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_skip_list_format.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/buffered_skip_list_writer.h"

IE_NAMESPACE_BEGIN(index);

class InMemTriValueSkipListReaderTest : public TriValueSkipListReaderTest
{
public:
    InMemTriValueSkipListReaderTest() {}
    virtual ~InMemTriValueSkipListReaderTest() {}

    DECLARE_CLASS_NAME(InMemTriValueSkipListReaderTest);

public:
    void TestCaseForGetPrevAndCurrentTTF();

protected:
    virtual SkipListReaderPtr PrepareReader(uint32_t itemCount, bool needFlush) override;

private:
    index::DocListSkipListFormatPtr mFormat;
    BufferedSkipListWriterPtr mWriter;    

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMTRIVALUESKIPLISTREADERTEST_H
