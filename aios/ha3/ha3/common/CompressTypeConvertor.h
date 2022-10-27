#ifndef ISEARCH_COMPRESSTYPECONVERTOR_H
#define ISEARCH_COMPRESSTYPECONVERTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(common);

class CompressTypeConvertor
{
public:
    CompressTypeConvertor();
    ~CompressTypeConvertor();
private:
    CompressTypeConvertor(const CompressTypeConvertor &);
    CompressTypeConvertor& operator=(const CompressTypeConvertor &);
public:
    static HaCompressType convertCompressType(
            const std::string &compressTypeStr);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(CompressTypeConvertor);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_COMPRESSTYPECONVERTOR_H
