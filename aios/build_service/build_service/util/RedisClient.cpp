// IMPORTANT: testcase will link FakeRedisClient, make sure link real RedisClient and
// pass all testcases after modify this file
#include "build_service/util/RedisClient.h"
using namespace std;

namespace build_service {
namespace util {
BS_LOG_SETUP(util, RedisClient);

RedisClient::RedisClient()
    : _context(NULL)
    , _isConnected(false)
{
}

RedisClient::~RedisClient() {
    if(_context) {   
        redisFree(_context);
    }
}

bool RedisClient::validate(redisReply* reply, RedisClient::ErrorCode &errorCode) {
    if (reply == NULL) {
        _lastError = "illegal command";
        errorCode = RC_ARG_ERROR;
        return false;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        _lastError = string(reply->str, reply->len);
        errorCode = RC_UNKNOWN;
        return false;
    }
    return true;
}

bool RedisClient::connect(const RedisInitParam &initParam, int timeout)
{
    struct timeval tv = {timeout, 0};
    _context = redisConnectWithTimeout(initParam.hostname.c_str(), initParam.port, tv);
    if(_context == NULL || _context->err) {
        if(_context) {
            _lastError = string(_context->errstr);
        }else{
            _lastError = "Command illegal or server error.";
        }
        return false;
    }
    
    redisReply *reply = (redisReply*) redisCommand(_context, "AUTH %s",
            initParam.password.c_str());
    ReplyGuard guard(reply);
    ErrorCode errorCode;
    if (!validate(reply, errorCode)) {
        return false;
    }
    _isConnected = true;
    return true;
}
 
int RedisClient::setHash(const string &key,
                          const string &fieldName,
                          const string &value,
                          RedisClient::ErrorCode &errorCode)
{
    if (!_isConnected) {
        errorCode = RC_UNCONNCTED;
        return 0;
    }
    
    assert(_context);
    redisReply *reply = (redisReply*) redisCommand(_context, "HSET %s %s %s",
            key.c_str(), fieldName.c_str(), value.c_str());
    ReplyGuard guard(reply);
    errorCode = RC_OK;
    if (!validate(reply, errorCode)) {
        return 0;
    }

    if (reply->type != REDIS_REPLY_INTEGER) {
        errorCode = RC_UNKNOWN;
        _lastError = "hset not return integer";
        return 0;
    }
    return reply->integer;
}

string RedisClient::getHash(const string &key, const string &fieldName,
                            RedisClient::ErrorCode &errorCode)
{
    if (!_isConnected) {
        errorCode = RC_UNCONNCTED;
        return "";
    }
        
    assert(_context);    
    redisReply *reply = (redisReply*) redisCommand(_context, "HGET %s %s",
            key.c_str(), fieldName.c_str());
    ReplyGuard guard(reply);
    errorCode = RC_OK;
    if (!validate(reply, errorCode)) {
        return "";
    }

    if (reply->type == REDIS_REPLY_NIL) {
        _lastError = "key does not exist or field is not present";
        errorCode = RC_HASH_FIELD_NONEXIST;
        return "";
    }

    if (reply->type != REDIS_REPLY_STRING) {
        _lastError = "HGET not return string";
        errorCode = RC_UNKNOWN;
        return "";        
    }

    return string(reply->str, reply->len);
}

map<string, string> RedisClient::getHash(const string &key, RedisClient::ErrorCode &errorCode) {
    if (!_isConnected) {
        errorCode = RC_UNCONNCTED;
        return {};
    }
    
    errorCode = RC_OK;    
    redisReply *reply = (redisReply*) redisCommand(_context, "HGETALL %s", key.c_str());
    ReplyGuard guard(reply);

    errorCode = RC_OK;
    if (!validate(reply, errorCode)) {
        return {};
    }

    if (reply->type != REDIS_REPLY_ARRAY) {
        _lastError = "HGETALL not return array";
        errorCode = RC_UNKNOWN;
        return {};
    }

    if (reply->elements == 0) {
        _lastError = "key does not exists";
        errorCode = RC_HASH_NONEXIST;
        return {};
    }

    if (reply->elements % 2  != 0) {
        _lastError = "HGETALL return array with odd elements count";
        errorCode = RC_UNKNOWN;
        return {};        
    }
    
    map<string, string> retMap;
    size_t count = reply->elements;
    for (size_t i = 0; i < count; i+=2) {
        redisReply* fieldReply = reply->element[i];
        if (fieldReply->type != REDIS_REPLY_STRING) {
            _lastError = "HGETALL return invalid field type";
            errorCode = RC_UNKNOWN;
            return {}; 
        }
        redisReply* valueReply = reply->element[i+1];
        if (valueReply->type != REDIS_REPLY_STRING) {
            _lastError = "HGETALL return invalid value type";
            errorCode = RC_UNKNOWN;
            return {}; 
        }
        retMap.insert(make_pair(string(fieldReply->str, fieldReply->len),
                                string(valueReply->str, valueReply->len)));
    }
    return retMap;
}

int RedisClient::removeKey(const string &key, RedisClient::ErrorCode &errorCode) {
    if (!_isConnected) {
        errorCode = RC_UNCONNCTED;
        return 0;
    }
    
    errorCode = RC_OK;
    redisReply *reply = (redisReply*) redisCommand(_context, "DEL %s", key.c_str());
    ReplyGuard guard(reply);
    errorCode = RC_OK;
    if (!validate(reply, errorCode)) {
        return 0;
    }

    if (reply->type != REDIS_REPLY_INTEGER) {
        _lastError = "DEL not return integer";
        errorCode = RC_UNKNOWN;
        return 0;
    }
    return reply->integer;
}

int RedisClient::expireKey(const string &key, int ttlInSecond, ErrorCode &errorCode) {
    if (!_isConnected) {
        errorCode = RC_UNCONNCTED;
        return 0;
    }
    
    errorCode = RC_OK;
    redisReply *reply = (redisReply*) redisCommand(_context, "EXPIRE %s %d",
            key.c_str(), ttlInSecond);
    ReplyGuard guard(reply);
    errorCode = RC_OK;
    if (!validate(reply, errorCode)) {
        return 0;
    }

    if (reply->type != REDIS_REPLY_INTEGER) {
        _lastError = "EXPIRE not return integer";
        errorCode = RC_UNKNOWN;
        return 0;
    }
    return reply->integer;    
}

}
}
