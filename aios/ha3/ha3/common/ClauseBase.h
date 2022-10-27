#ifndef ISEARCH_CLAUSEBASE_H
#define ISEARCH_CLAUSEBASE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>

BEGIN_HA3_NAMESPACE(common);

class ClauseBase
{
public:
    ClauseBase();
    virtual ~ClauseBase();
private:
    ClauseBase(const ClauseBase &);
    ClauseBase& operator=(const ClauseBase &);
public:
    void setOriginalString(const std::string& originalString) {
        _originalString = originalString;
    }
    const std::string& getOriginalString() const {
        return _originalString;
    }

    virtual void serialize(autil::DataBuffer &dataBuffer) const = 0;
    virtual void deserialize(autil::DataBuffer &dataBuffer) = 0;
    virtual std::string toString() const = 0;

    static std::string cluster2ZoneName(const std::string& clusterName);
protected:
    static std::string boolToString(bool flag);
protected:
    std::string _originalString;
private:
    HA3_LOG_DECLARE();
};


HA3_TYPEDEF_PTR(ClauseBase);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_CLAUSEBASE_H
