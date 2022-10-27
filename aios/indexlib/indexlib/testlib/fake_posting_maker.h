#ifndef __INDEXLIB_FAKE_POSTING_MAKER_H
#define __INDEXLIB_FAKE_POSTING_MAKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/testlib/fake_posting_iterator.h"

IE_NAMESPACE_BEGIN(testlib);

class FakePostingMaker
{
public:
    FakePostingMaker();
    ~FakePostingMaker();
public:
    static void makeFakePostingsDetail(const std::string &str,FakePostingIterator::Map &postingMap);
    static void makeFakePostingsSection(const std::string &str, 
        FakePostingIterator::Map &postingMap);
    
protected:
    static void parseOnePostingDetail(const std::string &postingStr, 
                                FakePostingIterator::Posting &posting);

    static void parseOnePostingSection(const std::string &postingStr,
        FakePostingIterator::Posting &posting);   

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakePostingMaker);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_POSTING_MAKER_H
