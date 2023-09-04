// Fake RedisClient implementation for testcase. link before real RedisClient
// Only support redis hash
#include "build_service/util/test/FakeRedisClient.h"

#include "autil/Lock.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace build_service { namespace util {

namespace {

autil::ThreadMutex fakeRedisMutex;
const std::string fakeRedisPassword = "Bstest123";

std::map<string, std::map<string, string>> fakeRedisMap;
std::map<string, int64_t> fakeRedisTtlMap;

} // namespace

void envictExpiredKey(const string& key)
{
    int64_t curTs = TimeUtility::currentTimeInSeconds();
    int64_t expireTs = std::numeric_limits<int64_t>::max();
    if (fakeRedisTtlMap.find(key) != fakeRedisTtlMap.end()) {
        expireTs = fakeRedisTtlMap[key];
    }
    if (curTs > expireTs) {
        fakeRedisMap.erase(key);
        fakeRedisTtlMap.erase(key);
    }
}

bool RedisClient::connect(const RedisInitParam& initParam, int timeout)
{
    autil::ScopedLock l(fakeRedisMutex);
    if (initParam.password == fakeRedisPassword) {
        _isConnected = true;
        return true;
    }
    return false;
}

int RedisClient::setHash(const string& key, const string& fieldName, const string& value,
                         RedisClient::ErrorCode& errorCode)
{
    autil::ScopedLock l(fakeRedisMutex);
    envictExpiredKey(key);
    if (!_isConnected) {
        errorCode = RC_UNCONNCTED;
        return 0;
    }
    bool exist = false;
    auto keyIter = fakeRedisMap.find(key);
    if (keyIter != fakeRedisMap.end()) {
        auto fieldIter = keyIter->second.find(fieldName);
        if (fieldIter != keyIter->second.end()) {
            exist = true;
        }
    }

    errorCode = RC_OK;
    fakeRedisMap[key][fieldName] = value;
    return exist ? 0 : 1;
}

string RedisClient::getHash(const string& key, const string& fieldName, RedisClient::ErrorCode& errorCode)
{
    autil::ScopedLock l(fakeRedisMutex);
    envictExpiredKey(key);
    if (!_isConnected) {
        errorCode = RC_UNCONNCTED;
        return "";
    }
    auto keyIter = fakeRedisMap.find(key);
    if (keyIter == fakeRedisMap.end()) {
        errorCode = RC_HASH_FIELD_NONEXIST;
        return "";
    }
    auto fieldIter = keyIter->second.find(fieldName);
    if (fieldIter == keyIter->second.end()) {
        errorCode = RC_HASH_FIELD_NONEXIST;
        return "";
    }
    errorCode = RC_OK;
    return fieldIter->second;
}

map<string, string> RedisClient::getHash(const string& key, RedisClient::ErrorCode& errorCode)
{
    autil::ScopedLock l(fakeRedisMutex);
    envictExpiredKey(key);
    if (!_isConnected) {
        errorCode = RC_UNCONNCTED;
        return {};
    }
    errorCode = RC_OK;
    auto keyIter = fakeRedisMap.find(key);
    if (keyIter == fakeRedisMap.end()) {
        errorCode = RC_HASH_NONEXIST;
        return {};
    }
    return keyIter->second;
}

int RedisClient::removeKey(const string& key, RedisClient::ErrorCode& errorCode)
{
    autil::ScopedLock l(fakeRedisMutex);
    envictExpiredKey(key);
    if (!_isConnected) {
        errorCode = RC_UNCONNCTED;
        return {};
    }
    errorCode = RC_OK;
    fakeRedisMap.erase(key);
    fakeRedisTtlMap.erase(key);
    return 0;
}

int RedisClient::expireKey(const string& key, int ttlInSecond, ErrorCode& errorCode)
{
    autil::ScopedLock l(fakeRedisMutex);
    envictExpiredKey(key);
    if (!_isConnected) {
        errorCode = RC_UNCONNCTED;
        return 0;
    }
    errorCode = RC_OK;
    auto keyIter = fakeRedisMap.find(key);
    if (keyIter == fakeRedisMap.end()) {
        errorCode = RC_HASH_NONEXIST;
        return 0;
    }
    int64_t curTs = TimeUtility::currentTimeInSeconds();
    fakeRedisTtlMap[key] = curTs + ttlInSecond;
    return 1;
}

}} // namespace build_service::util
