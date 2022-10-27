#ifndef ISEARCH_INDEXINFOHELPER_H
#define ISEARCH_INDEXINFOHELPER_H

#include <string>

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(config);

class IndexInfoHelper
{
public:
    virtual ~IndexInfoHelper() {}
public:
    virtual indexid_t getIndexId(const std::string &indexName) const = 0;
    virtual int32_t getFieldPosition(const std::string &indexName,
					const std::string &fieldname) const = 0;
    virtual std::vector<std::string>  getFieldList(const std::string &indexName) const = 0;
};

HA3_TYPEDEF_PTR(IndexInfoHelper);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_INDEXINFOHELPER_H
