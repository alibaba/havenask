#ifndef __INDEXLIB_PRIMARY_KEY_PAIR_SEGMENT_ITERATOR_H
#define __INDEXLIB_PRIMARY_KEY_PAIR_SEGMENT_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class PrimaryKeyPairSegmentIterator
{
public:
    typedef typename PrimaryKeyFormatter<Key>::PKPair PKPair;

public:
    PrimaryKeyPairSegmentIterator()
    { }

    virtual ~PrimaryKeyPairSegmentIterator()
    { }

public:
    virtual void Init(const file_system::FileReaderPtr& fileReader) = 0;
    virtual bool HasNext() const = 0;
    virtual void Next(PKPair& pkPair) = 0;
    virtual void GetCurrentPKPair(PKPair& pair) const = 0;
    virtual uint64_t GetPkCount() const = 0;

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_PAIR_SEGMENT_ITERATOR_H
