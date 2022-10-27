#ifndef ISEARCH_BS_TASKIDENTIFIER_H
#define ISEARCH_BS_TASKIDENTIFIER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace common {

class TaskIdentifier
{
public:
    TaskIdentifier();
    ~TaskIdentifier();
private:
    TaskIdentifier(const TaskIdentifier &);
    TaskIdentifier& operator=(const TaskIdentifier &);
public:
    virtual bool fromString(const std::string &content);
    virtual std::string toString() const;
    
    void setTaskId(const std::string& id) { _taskId = id; }
    std::string getTaskId() const { return _taskId; }

    void setTaskName(const std::string& taskName) {
        setValue(FIELDNAME_TASKNAME, taskName);
    }
    bool getTaskName(std::string& taskName) const {
        return getValue(FIELDNAME_TASKNAME, taskName);
    }
    
    void setClusterName(const std::string& clusterName) {
        setValue(FIELDNAME_CLUSTERNAME, clusterName);
    }
    bool getClusterName(std::string& clusterName) const {
        return getValue(FIELDNAME_CLUSTERNAME, clusterName);
    }

public:
    void setValue(const std::string& key, const std::string& value);
    bool getValue(const std::string& key, std::string& value) const;

protected:
    typedef std::map<std::string, std::string> KeyValueMap;
    std::string _taskId;
    KeyValueMap _kvMap;

public:
    static const std::string FIELD_SEPARATOR;
    static const std::string FIELDNAME_SEPARATOR;
    static const std::string FIELDNAME_CLUSTERNAME;
    static const std::string FIELDNAME_TASKNAME;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskIdentifier);

}
}

#endif //ISEARCH_BS_TASKIDENTIFIER_H
