#ifndef ISEARCH_TURING_QRS_TURING_URL_TRANSFORM_H
#define ISEARCH_TURING_QRS_TURING_URL_TRANSFORM_H

#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <suez/turing/proto/Search.pb.h>

BEGIN_HA3_NAMESPACE(turing);

class TuringUrlTransform
{
public:
    bool init();
    suez::turing::GraphRequest* transform(const std::string& url) const;

    void setBizNameKey(const std::string& key);
    const std::string& getBizNameKey() const;

    void setSrcKey(const std::string& key);
    const std::string& getSrcKey() const;

    void setTimeout(int64_t t);
    int64_t getTimeout() const;
private:
    std::string _bizNameKey{"s"};
    std::string _srcKey{"src"};
    int64_t _timeout{500};
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_TURING_QRS_TURING_URL_TRANSFORM_H
