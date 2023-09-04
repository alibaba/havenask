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
#ifndef CARBON_SKYLINE_TYPES_H
#define CARBON_SKYLINE_TYPES_H

#include "common/common.h"
#include "common/HttpUtil.h" // StringKVS

BEGIN_CARBON_NAMESPACE(skyline);

JSONIZABLE_CLASS(Auth)
{
public:
    Auth() {}
    Auth(const std::string& appName_, const std::string& account_, const std::string& sig, int64_t tm) :
        appName(appName_), account(account_), signature(sig), timestamp(tm) {}

    JSONIZE() {
        JSON_FIELD(appName);
        JSON_FIELD(account);
        JSON_FIELD(signature);
        JSON_FIELD(timestamp);
    }

    std::string appName;
    std::string account;
    std::string signature;
    int64_t timestamp;
};

JSONIZABLE_CLASS(Operator)
{
public:
    Operator() {}
    Operator(const std::string& type_, const std::string& nick_, const std::string& workerId_) :
        type(type_), nick(nick_), workerId(workerId_) {}

    JSONIZE() {
        JSON_FIELD(type);
        JSON_FIELD(nick);
        JSON_FIELD(workerId);
    }

    std::string type;
    std::string nick;
    std::string workerId;
};

template <typename T>
struct Request : public autil::legacy::Jsonizable
{
    Request() {}
    Request(const Auth& auth_, const T& item_, const Operator& op) : auth(auth_), oper(op), item(item_) {}

    JSONIZE() {
        JSON_FIELD(auth);
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            json.Jsonize("operator", oper, oper);
        }
        JSON_FIELD(item);
    }

    Auth auth; 
    Operator oper;
    T item;
};

#define APP_USE_TYPE_PRE "PRE_PUBLISH"
#define APP_USE_TYPE_PUB "PUBLISH"

JSONIZABLE_CLASS(UpdateProperty)
{
public:
    JSONIZE() {
        JSON_FIELD(sn);
        JSON_FIELD(propertyMap);
    }

    std::string sn;
    common::StringKVS propertyMap;
};

inline UpdateProperty setAppUseTypeReq(const std::string& sn, const std::string& type) {
    UpdateProperty req;
    req.sn = sn;
    req.propertyMap["appUseType"] = type;
    return req;
}

#define BATCH_UPDATE_MAX 100 // skyline limit 

JSONIZABLE_CLASS(BatchUpdate)
{
public:
    JSONIZE() {
        JSON_FIELD(snList);
    }

    std::vector<std::string> snList;
};

JSONIZABLE_CLASS(UpdateTo) {
public:
    UpdateTo(const std::string& v = "") : updateTo(v) {}

    JSONIZE() {
        JSON_FIELD(updateTo);
    }

    std::string updateTo;
};

struct BatchUpdateGroup : public BatchUpdate
{
    BatchUpdateGroup(const std::string& group = std::string()) : appGroup(group) {}

    JSONIZE() {
        BatchUpdate::Jsonize(json);
        JSON_FIELD(appGroup);
    }

    UpdateTo appGroup;
};

#define SERVER_STATE_OFFLINE "working_offline"
#define SERVER_STATE_ONLINE "working_online"

struct BatchUpdateState : public BatchUpdate
{
    BatchUpdateState(const std::string& state = std::string()) : appServerState(state) {}

    JSONIZE() {
        BatchUpdate::Jsonize(json);
        JSON_FIELD(appServerState);
    }

    UpdateTo appServerState;
};

#define NUMBER_IN_PAGE 1000 // skyline max limit to 1000

JSONIZABLE_CLASS(Query)
{
public:
    Query() {}
    Query(const std::string& from, const std::string& select, const std::string& cond, int page = 1, int number = NUMBER_IN_PAGE, bool needTotal = true) {
        this->from = from;
        this->select = select;
        this->condition = cond;
        this->page = page;
        this->num = number;
        this->needTotal = needTotal;
    }

    JSONIZE() {
        JSON_FIELD(from);
        JSON_FIELD(select);
        JSON_FIELD(condition);
        JSON_FIELD(page);
        JSON_FIELD(num);
        JSON_FIELD(needTotal);
    }

    std::string from;
    std::string select;
    std::string condition;
    int page;
    int num;
    bool needTotal;
};

struct BaseResult : public autil::legacy::Jsonizable {
    BaseResult() : success(false), errorCode(0) {}

    JSONIZE() {
        JSON_FIELD(success);
        JSON_FIELD(errorCode);
        JSON_FIELD(errorMessage);
        JSON_FIELD(exceptionName);
    }

    bool success;
    int errorCode;
    std::string errorMessage;
    std::string exceptionName;
};

template <typename T>
struct Result : public BaseResult
{
    JSONIZE() {
        BaseResult::Jsonize(json);
        JSON_FIELD(value);
    }

    T value;
};

template <>
struct Result<void> : public BaseResult {};

template <typename T>
JSONIZABLE_CLASS(QueryResultT)
{
public:
    QueryResultT() : totalCount(0), hasMore(false) {}

    JSONIZE() {
        JSON_FIELD(totalCount);
        JSON_FIELD(hasMore);
        JSON_FIELD(itemList);
    }

    int totalCount;
    bool hasMore;
    std::vector<T> itemList;
};

JSONIZABLE_CLASS(Server)
{
public:
    JSONIZE() {
        JSON_FIELD(sn);
        JSON_FIELD(ip);
        JSON_FIELD(app_server_state);
        JSON_FIELD(app_use_type);
    }

    std::string sn;
    std::string ip;
    std::string app_server_state;
    std::string app_use_type;
};

END_CARBON_NAMESPACE(skyline);

#endif
