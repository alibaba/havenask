#pragma once

#include <string>
#include <stdlib.h>

class TestEnvScope {
public:
    TestEnvScope(const std::string &key, const std::string &value)
        : _key(key)
    {
        auto *oldValue = getenv(key.c_str());
        if (oldValue != nullptr) {
            _hasValue = true;
            _oldValue = oldValue;
        }
        setenv(key.c_str(), value.c_str(), 1);
    }
    ~TestEnvScope() {
        if (_hasValue) {
            setenv(_key.c_str(), _oldValue.c_str(), 1);
        } else {
            unsetenv(_key.c_str());
        }
    }
private:
    bool _hasValue = false;
    std::string _key;
    std::string _oldValue;
};
