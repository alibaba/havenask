#ifndef ISEARCH_BS_CUSTOMMERGERTASKITEM_H
#define ISEARCH_BS_CUSTOMMERGERTASKITEM_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace custom_merger {

class CustomMergerField : public autil::legacy::Jsonizable
{
public:
    enum FieldType {
        INDEX,
        ATTRIBUTE
    };
    CustomMergerField() {}
    CustomMergerField(const std::string& _fieldName, FieldType _fieldType)
        : fieldName(_fieldName)
        , fieldType(_fieldType)
    {
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    std::string fieldName;
    FieldType fieldType;
};


class CustomMergerTaskItem : public autil::legacy::Jsonizable
{
public:
    CustomMergerTaskItem();
    ~CustomMergerTaskItem();
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    uint64_t generateTaskKey() const;
public:
    std::vector<CustomMergerField> fields;
    double cost;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMergerTaskItem);

}
}

#endif //ISEARCH_BS_CUSTOMMERGERTASKITEM_H
