#ifndef ISEARCH_MAKEFAKEMETA_H_
#define ISEARCH_MAKEFAKEMETA_H_

#include <ha3/isearch.h>
#include <ha3/index/index.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <ha3_sdk/testlib/index/FakeInDocSectionMeta.h>

IE_NAMESPACE_BEGIN(index);

class FakeMetaMaker
{
public:
    static void makeFakeMeta(std::string str,
        FakeTextIndexReader::DocSectionMap &docSecMetaMap);

private:
    static void parseDocMeta(std::string, FakeInDocSectionMeta::DocSectionMeta
        &docSecMeta);
    static void parseFieldMeta(std::string, int32_t,
        FakeTextIndexReader::FieldAndSectionMap&); 
    HA3_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif

