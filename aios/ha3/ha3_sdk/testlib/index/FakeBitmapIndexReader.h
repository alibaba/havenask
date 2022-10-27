#ifndef ISEARCH_FAKEBITMAPINDEXREADER_H
#define ISEARCH_FAKEBITMAPINDEXREADER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>

IE_NAMESPACE_BEGIN(index);

class FakeBitmapIndexReader : public FakeTopIndexReader 
{
public:
    FakeBitmapIndexReader(const std::string &datas);
    ~FakeBitmapIndexReader();
private:
    FakeBitmapIndexReader(const FakeBitmapIndexReader &);
    FakeBitmapIndexReader& operator=(const FakeBitmapIndexReader &);
public:
    virtual PostingIterator *Lookup(const common::Term& term,
                                    uint32_t statePoolSize = 1000,
                                    PostingType type = pt_default,
                                    autil::mem_pool::Pool *sessionPool = NULL);

public:
    FakeTextIndexReader::Map _map;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeBitmapIndexReader);

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEBITMAPINDEXREADER_H
