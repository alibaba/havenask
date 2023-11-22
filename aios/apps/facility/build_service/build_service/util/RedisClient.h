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

#include <hiredis/hiredis.h>
#include <map>
#include <stdint.h>
#include <string>

#include "autil/StringUtil.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace util {

struct RedisInitParam {
    RedisInitParam() {}
    RedisInitParam(const std::string& _host, uint16_t _port, const std::string& _password)
        : hostname(_host)
        , port(_port)
        , password(_password)
    {
    }

    std::string toString() const
    {
        return "hostname : " + hostname + ", port:" + autil::StringUtil::toString(port) + ", pass:" + password + ".";
    }

    std::string hostname;
    uint16_t port;
    std::string password;
};

class ReplyGuard
{
public:
    ReplyGuard(redisReply* reply) : _reply(reply) {}
    ~ReplyGuard() { freeReplyObject(_reply); }

private:
    ReplyGuard(const ReplyGuard&);
    ReplyGuard& operator=(const ReplyGuard&);

private:
    redisReply* _reply;
};

class RedisClient
{
public:
    RedisClient();
    ~RedisClient();
    enum ErrorCode {
        RC_OK = 0,
        RC_SET_FAIL,
        RC_CONNECT_FAIL,
        RC_HASH_FIELD_NONEXIST,
        RC_HASH_NONEXIST,
        RC_ARG_ERROR,
        RC_UNCONNCTED,
        RC_UNKNOWN
    };

private:
    RedisClient(const RedisClient&);
    RedisClient& operator=(const RedisClient&);

public:
    bool connect(const RedisInitParam& initParam, int timeout = 30);

    // return 1 if field is a new field in the hash and value was set
    // return 0 if field already exists in the hash and the value was updated.
    int setHash(const std::string& key, const std::string& fieldName, const std::string& value, ErrorCode& errorCode);

    std::string getHash(const std::string& key, const std::string& fieldName, ErrorCode& errorCode);

    std::map<std::string, std::string> getHash(const std::string& key, ErrorCode& errorCode);

    int removeKey(const std::string& key, ErrorCode& errorCode);

    int expireKey(const std::string& key, int ttlInSecond, ErrorCode& errorCode);

    const std::string& getLastError() { return _lastError; }

private:
    bool validate(redisReply* reply, ErrorCode& errorCode);

private:
    std::string _lastError;
    redisContext* _context;
    bool _isConnected;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RedisClient);

}} // namespace build_service::util
