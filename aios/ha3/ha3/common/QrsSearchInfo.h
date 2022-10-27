#ifndef ISEARCH_QRSSEARCHINFO_H
#define ISEARCH_QRSSEARCHINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <autil/DataBuffer.h>

BEGIN_HA3_NAMESPACE(common);

struct QrsSearchInfo
{
public:
    QrsSearchInfo() {}
    ~QrsSearchInfo() {}

public:
    const std::string& getQrsSearchInfo() const {
        return qrsLogInfo;
    }
    void appendQrsInfo(const std::string& info) {
        qrsLogInfo.append(info);
    }
    void reset() {
        qrsLogInfo.clear();
    }
    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(qrsLogInfo);
    }
    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(qrsLogInfo);
    }
    bool operator == (const QrsSearchInfo &other) const {
        return qrsLogInfo == other.qrsLogInfo;
    }
public:
    std::string qrsLogInfo;
};

HA3_TYPEDEF_PTR(QrsSearchInfo);

END_HA3_NAMESPACE(common);

#endif 
