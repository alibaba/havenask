#ifndef ISEARCH_JSONVALIDATOR_H
#define ISEARCH_JSONVALIDATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <string>
BEGIN_HA3_NAMESPACE(util);

class JsonValidator
{
public:
    JsonValidator();
    ~JsonValidator();
public:
    static bool validate(const std::string &fileName);
private:
    static bool validateJsonString(const std::string &content,
                                   std::string &errDes);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(JsonValidator);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_JSONVALIDATOR_H
