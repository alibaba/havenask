#ifndef ISEARCH_INDEX_FAKENUMBERINDEXREADER_H
#define ISEARCH_INDEX_FAKENUMBERINDEXREADER_H

#include <ha3/index/index.h>
#include <vector>
#include <string>

#include <indexlib/common_define.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3_sdk/testlib/index/FakeTopIndexReader.h>
#include <ha3/util/Log.h>

IE_NAMESPACE_BEGIN(index);

class FakeNumberIndexReader : public FakeTopIndexReader
{
public:
    typedef std::vector<int32_t> NumberValues;
    typedef std::tr1::shared_ptr<std::vector<int32_t> > NumberValuesPtr;

    FakeNumberIndexReader() : _numberValuePtr(new NumberValues) {}
    virtual ~FakeNumberIndexReader(){}

    //PostingIteratorPtr Lookup(const IE_NAMESPACE(util)::Term &term, bool hasPosition);
    PostingIterator *Lookup(const common::Term& term, uint32_t statePoolSize = 1000,
                            PostingType type = pt_default, autil::mem_pool::Pool *pool = NULL);

    void setNumberValuesFromString(const std::string &numberValues);
    NumberValuesPtr getNumberValues();
    size_t size();
    void clear();
    const SectionAttributeReader* GetSectionReader(const std::string& indexName) const;
private:
    void parseOneLine(const std::string &aLine);
private:
    NumberValuesPtr _numberValuePtr;
private:
    HA3_LOG_DECLARE();
};

IE_NAMESPACE_END(index);
#endif
