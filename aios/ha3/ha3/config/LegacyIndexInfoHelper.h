#ifndef ISEARCH_LEGACYINDEXINFOHELPER_H
#define ISEARCH_LEGACYINDEXINFOHELPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/IndexInfoHelper.h>
#include <suez/turing/expression/util/IndexInfos.h>

BEGIN_HA3_NAMESPACE(config);

class LegacyIndexInfoHelper : public IndexInfoHelper
{
public:
    LegacyIndexInfoHelper();
    LegacyIndexInfoHelper(const suez::turing::IndexInfos *indexInfos);
    virtual ~LegacyIndexInfoHelper();
public:
    virtual indexid_t getIndexId(const std::string &indexName) const;
    virtual int32_t getFieldPosition(const std::string &indexName,
            const std::string &fieldname) const;
    virtual std::vector<std::string> getFieldList(const std::string &indexName) const;
    void setIndexInfos(const suez::turing::IndexInfos *indexInfos);
private:
    const suez::turing::IndexInfos *_indexInfos;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(config);

#endif //ISEARCH_LEGACYINDEXINFOHELPER_H
