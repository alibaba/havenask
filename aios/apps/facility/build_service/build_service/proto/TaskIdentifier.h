/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <map>
#include <string>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace proto {

class TaskIdentifier
{
public:
    TaskIdentifier();
    ~TaskIdentifier();

private:
    TaskIdentifier(const TaskIdentifier&);
    TaskIdentifier& operator=(const TaskIdentifier&);

public:
    virtual bool fromString(const std::string& content);
    virtual std::string toString() const;

    void setTaskId(const std::string& id) { _taskId = id; }
    std::string getTaskId() const { return _taskId; }

    void setTaskName(const std::string& taskName) { setValue(FIELDNAME_TASKNAME, taskName); }
    bool getTaskName(std::string& taskName) const { return getValue(FIELDNAME_TASKNAME, taskName); }

    void setClusterName(const std::string& clusterName) { setValue(FIELDNAME_CLUSTERNAME, clusterName); }
    bool getClusterName(std::string& clusterName) const { return getValue(FIELDNAME_CLUSTERNAME, clusterName); }

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

}} // namespace build_service::proto
