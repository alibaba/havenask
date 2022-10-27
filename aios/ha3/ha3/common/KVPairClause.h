#ifndef ISEARCH_KVPAIRCLAUSE_H
#define ISEARCH_KVPAIRCLAUSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(common);

// do not need serialize
class KVPairClause 
{
public:
    KVPairClause();
    ~KVPairClause();
private:
    KVPairClause(const KVPairClause &);
    KVPairClause& operator=(const KVPairClause &);
public:
    const std::string &getOriginalString() const { return _originalString; }
    void setOriginalString(const std::string &originalString) { _originalString = originalString; }
    std::string toString() const { return _originalString; }
private:
    std::string _originalString;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KVPairClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_KVPAIRCLAUSE_H
