#ifndef ISEARCH_MAKEFAKEPOSTINGS_H_
#define ISEARCH_MAKEFAKEPOSTINGS_H_

#include <ha3/isearch.h>
#include <ha3/index/index.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>

IE_NAMESPACE_BEGIN(index);

class FakePostingMaker
{
public:
    /* 
     * term^termpayload:docid^docpayload[position_pospayload,position_pospayload,...]
     */ 
    static void makeFakePostingsDetail(const std::string &str,FakeTextIndexReader::Map &postingMap);
    static void makeFakePostingsSection(const std::string &str, 
        FakeTextIndexReader::Map &postingMap);
    
protected:
    static void parseOnePostingDetail(const std::string &postingStr, 
                                FakeTextIndexReader::Posting &posting);

    static void parseOnePostingSection(const std::string &postingStr,
        FakeTextIndexReader::Posting &posting);   

private:
    HA3_LOG_DECLARE();
};

IE_NAMESPACE_END(index);
#endif
