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

#include <any>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

#include "future_lite/Log.h"

namespace future_lite {

class Executor;

namespace detail {

class ExecutorCreatorBase {
public:
    class Parameters {
    public:
        Parameters() = default;
        template <typename T>
        Parameters &Set(const std::string &key, T value);
        template <typename T>
        std::optional<T> Get(const std::string &key) const;

        Parameters &SetThreadNum(size_t threadNum);
        std::optional<size_t> GetThreadNum() const;
        Parameters &SetExecutorName(std::string name);
        std::optional<std::string> GetExecutorName() const;

    private:
        std::map<std::string, std::any> _params;
    };

    ExecutorCreatorBase() = default;
    virtual ~ExecutorCreatorBase() {}

    virtual std::unique_ptr<future_lite::Executor> Create(const Parameters &param) = 0;

    static void Register(const std::string &type, std::unique_ptr<detail::ExecutorCreatorBase> creator);
};

} // namespace detail

class ExecutorCreator {
private:
    ExecutorCreator() = default;

public:
    using Parameters = detail::ExecutorCreatorBase::Parameters;
    static constexpr const char *NAME = "name";
    static constexpr const char *THREAD_NUM = "thread_num";

    ~ExecutorCreator() = default;
    ExecutorCreator(const ExecutorCreator &) = delete;
    ExecutorCreator &operator=(const ExecutorCreator &) = delete;

public:
    static std::unique_ptr<future_lite::Executor> Create(const std::string &type, const Parameters &parameters) {
        auto &inst = Instance();
        return inst.DoCreate(type, parameters);
    }
    static bool HasExecutor(const std::string &type) {
        auto &inst = Instance();
        return inst.DoHasExecutor(type);
    }

private:
    static ExecutorCreator &Instance();

    std::unique_ptr<future_lite::Executor> DoCreate(const std::string &type, const Parameters &parameters);
    bool DoHasExecutor(const std::string &type) const;
    void Register(const std::string &type, std::unique_ptr<detail::ExecutorCreatorBase> creator);

private:
    mutable std::mutex _mutex;
    std::map</*type*/ std::string, std::unique_ptr<detail::ExecutorCreatorBase>> _creators;

    static ExecutorCreator globalInstance;

    friend class detail::ExecutorCreatorBase;
};

namespace detail {

template <typename T>
inline ExecutorCreatorBase::Parameters &ExecutorCreatorBase::Parameters::Set(const std::string &key, T value) {
    _params[key] = std::move(value);
    return *this;
}

template <typename T>
inline std::optional<T> ExecutorCreatorBase::Parameters::Get(const std::string &key) const {
    auto iter = _params.find(key);
    if (iter == _params.end()) {
        return std::nullopt;
    }
    try {
        return std::any_cast<T>(iter->second);
    } catch (std::bad_any_cast &e) {
        std::fprintf(stderr, "get param[%s] fail: cast error: %s\n", key.c_str(), e.what());
        return std::nullopt;
    }
}

inline ExecutorCreatorBase::Parameters &ExecutorCreatorBase::Parameters::SetThreadNum(size_t threadNum) {
    return Set(ExecutorCreator::THREAD_NUM, threadNum);
}

inline std::optional<size_t> ExecutorCreatorBase::Parameters::GetThreadNum() const {
    return Get<size_t>(ExecutorCreator::THREAD_NUM);
}

inline ExecutorCreatorBase::Parameters &ExecutorCreatorBase::Parameters::SetExecutorName(std::string name) {
    return Set(ExecutorCreator::NAME, name);
}

inline std::optional<std::string> ExecutorCreatorBase::Parameters::GetExecutorName() const {
    return Get<std::string>(ExecutorCreator::NAME);
}

} // namespace detail

} // namespace future_lite
