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

#include <cassert>
#include <functional>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <vector>

// TODO: replace with <source_location> when we are ready for c++20
#include <experimental/source_location>

#include "autil/CommonMacros.h"
#include "autil/NoCopyable.h"
#include "autil/result/ForwardList.h"

namespace autil::result {

using SourceLocation = std::experimental::source_location;

class Error {
public:
    Error() = default;
    virtual ~Error() = default;

public:
    virtual std::unique_ptr<Error> clone() const = 0;

    /**
     * @brief Get the message attached to this error object.
     *
     * @return std::string
     */
    virtual std::string message() const = 0;

    /**
     * @brief Get the stack trace attached to this error object.
     *
     * @return const std::string&
     */
    virtual std::string get_stack_trace() const;

    /**
     * @brief Get the real c++ typename of this error object.
     *
     * @return std::string
     */
    std::string get_runtime_type_name() const;

    /**
     * @brief Macro specific method.
     */
    void append_stack_frame(const SourceLocation &loc) { _locs.emplace_back(loc); }

protected:
    ForwardList<SourceLocation, 8> _locs;
};

struct UnwrapFailedException : public std::runtime_error {
    std::unique_ptr<Error> err;
    std::string msg;

    UnwrapFailedException(std::unique_ptr<Error> &&err, const SourceLocation &loc = SourceLocation::current())
        : std::runtime_error{""}, err{std::move(err)} {
        this->err->append_stack_frame(loc);
        msg = this->err->get_stack_trace();
    }

    const char *what() const noexcept override { return msg.data(); }
};

struct NoStackFrameFlag {};

struct ResultBase {};

template <typename Self, typename T>
class ResultApiImpl : public ResultBase {
public:
    /**
     * @brief Check and get a pointer to the stored error, if the error satisfies ET.
     *
     * @return const ET*
     */
    template <typename ET, typename = typename std::enable_if_t<std::is_base_of_v<Error, ET>>>
    inline const ET *as() const noexcept {
        if (self()->is_ok()) {
            return nullptr;
        }
        return dynamic_cast<const ET *>(&self()->get_error());
    }

    /**
     * @brief Force accessing the value. If the result is an error,
     *        an exception corresponding to current error will be thrown.
     *
     * @param loc
     * @return T
     */
    [[gnu::always_inline]] inline T unwrap(const SourceLocation &loc = SourceLocation::current()) {
        self()->unwrap_check(loc);
        if constexpr (!std::is_same_v<T, void>) {
            return std::forward<T>(self()->get());
        }
    }

protected:
    [[gnu::always_inline]] inline void unwrap_check(const SourceLocation &loc) {
        if (unlikely(self()->is_err())) {
            throw UnwrapFailedException(self()->steal_error(), loc);
        }
    }

protected:
    [[gnu::always_inline]] const Self *self() const noexcept { return static_cast<const Self *>(this); }

    [[gnu::always_inline]] Self *self() noexcept { return static_cast<Self *>(this); }
};

template <typename Self, typename T, typename E, bool = std::is_copy_constructible_v<T>>
class ResultApi : public ResultApiImpl<Self, T> {
public:
    using ResultApiImpl<Self, T>::ResultApiImpl;

    Self clone(const SourceLocation &loc = SourceLocation::current()) const {
        if (self()->is_err()) {
            return Self(std::unique_ptr<E>(static_cast<E *>(self()->get_error().clone().release())), loc);
        } else {
            return self()->get();
        }
    }

private:
    using ResultApiImpl<Self, T>::self;
};

template <typename Self, typename E>
class ResultApi<Self, void, E, false> : public ResultApiImpl<Self, void> {
public:
    using ResultApiImpl<Self, void>::ResultApiImpl;

    Self clone(const SourceLocation &loc = SourceLocation::current()) const {
        if (self()->is_err()) {
            return Self(std::unique_ptr<E>(static_cast<E *>(self()->get_error().clone().release())), loc);
        } else {
            return {};
        }
    }

private:
    using ResultApiImpl<Self, void>::self;
};

template <typename Self, typename T, typename E>
class ResultApi<Self, T, E, false> : public ResultApiImpl<Self, T> {
public:
    using ResultApiImpl<Self, T>::ResultApiImpl;

private:
    using ResultApiImpl<Self, T>::self;
};

template <typename E, typename = std::enable_if_t<std::is_base_of_v<Error, E>>>
struct ErrorResult : public ResultApi<ErrorResult<E>, void, E> {
public:
    using StoredType = void;
    using ValueType = void;
    using DeclType = void;
    using ErrorType = std::unique_ptr<E>;

    ErrorResult() = default;
    ErrorResult(std::unique_ptr<E> &&error, const SourceLocation &loc = SourceLocation::current())
        : _error{std::move(error)} {}

    inline constexpr bool is_ok() const noexcept { return false; }
    inline constexpr bool is_err() const noexcept { return true; }

    inline constexpr void get() const noexcept { assert(false); }
    inline constexpr void steal_value() noexcept { assert(false); }

    inline const E &get_error() const noexcept { return *_error; }
    inline E &get_error() noexcept { return *_error; }
    inline std::unique_ptr<E> steal_error() noexcept { return std::move(_error); }

private:
    std::unique_ptr<E> _error;
};

namespace detail {
template <typename T>
struct ResultStoredTypeTraits {
    using Type = T;
    using ValueType = T;
};

template <typename T>
struct ResultStoredTypeTraits<T &> {
    using Type = std::reference_wrapper<T>;
    using ValueType = T;
};

template <typename T>
struct IsUniquePtrOfError {
    static constexpr bool Value = false;
};

template <typename T>
struct IsUniquePtrOfError<std::unique_ptr<T>> {
    static constexpr bool Value = std::is_base_of_v<Error, T>;
};

template <typename T, bool = std::is_base_of_v<ResultBase, std::decay_t<T>>>
struct ResultExprClassifier {
    static constexpr bool IsResult = true;
    static constexpr bool IsError = false;
    using ReturnType = typename std::decay_t<T>::DeclType;
};

template <typename T>
struct ResultExprClassifier<T, false> {
    static constexpr bool IsResult = false;
    static constexpr bool IsError = IsUniquePtrOfError<std::decay_t<T>>::Value;
    using ReturnType = void;
};

template <typename T>
struct Resultify {
    template <typename E>
    static auto Apply(E &&e) {
        if constexpr (ResultExprClassifier<E>::IsResult) {
            return std::forward<E>(e);
        } else if constexpr (ResultExprClassifier<E>::IsError) {
            return ErrorResult<typename std::decay_t<E>::value_type>(std::forward<E>(e));
        }
    }
};

enum class Discriminant : uint8_t {
    None = 0,
    Ok = 1,
    Err = 2,
};

} // namespace detail

template <typename T = void>
class [[nodiscard]] Result : public ResultApi<Result<T>, T, Error> {
public:
    template <typename U>
    friend class Result;

    using StoredType = typename detail::ResultStoredTypeTraits<T>::Type;
    using ValueType = typename detail::ResultStoredTypeTraits<T>::ValueType;
    using DeclType = T;
    using ErrorType = std::unique_ptr<Error>;

    Result(Result &&other) : _what{move_from(std::move(other))} {}
    template <typename U, typename = typename std::enable_if_t<std::is_convertible_v<U, T>>>
    Result(Result<U> &&other) : _what{move_from(std::move(other))} {}
    Result() : _stored{StoredType()}, _what{detail::Discriminant::Ok} {}
    Result(ValueType &&v) : _stored{std::forward<T>(v)}, _what{detail::Discriminant::Ok} {}
    Result(const ValueType &v) : _stored{v}, _what{detail::Discriminant::Ok} {}
    template <typename... Args, typename = std::enable_if_t<std::is_constructible_v<T, Args...>>>
    Result(Args &&...args) : _stored{std::forward<Args>(args)...}, _what{detail::Discriminant::Ok} {}
    template <typename E, typename = std::enable_if_t<std::is_base_of_v<Error, E>>>
    Result(std::unique_ptr<E> &&e, NoStackFrameFlag) noexcept
        : _error{std::move(e)}, _what{detail::Discriminant::Err} {}
    template <typename E, typename = std::enable_if_t<std::is_base_of_v<Error, E>>>
    Result(std::unique_ptr<E> &&e, const SourceLocation &loc = SourceLocation::current()) noexcept
        : _error{std::move(e)}, _what{detail::Discriminant::Err} {
        _error->append_stack_frame(loc);
    }
    template <typename E>
    Result(ErrorResult<E> &&e, const SourceLocation &loc = SourceLocation::current()) noexcept
        : Result{e.steal_error(), loc} {}
    ~Result() { destroy(); }
    Result &operator=(Result &&other) {
        destroy();
        _what = move_from(std::move(other));
        return *this;
    }
    template <typename U>
    Result &operator=(Result<U> &&other) {
        destroy();
        _what = move_from(std::move(other));
        return *this;
    }

public:
    /* Predicates */

    /**
     * @brief Check whether the result is ok.
     *
     * @return true
     * @return false
     */
    [[nodiscard]] inline bool is_ok() const noexcept { return _what == detail::Discriminant::Ok; }

    /**
     * @brief Check whether the result is an error.
     *
     * @return true
     * @return false
     */
    [[nodiscard]] inline bool is_err() const noexcept { return _what == detail::Discriminant::Err; }

public:
    /* Accessors */

    /**
     * @brief Get the stored value.
     * @note  If the result is an error, the behaviour is undefined.
     *
     * @return ValueType&
     */
    inline ValueType &get() noexcept {
        assert(is_ok());
        return _stored;
    }

    /**
     * @brief Get the stored value.
     * @note  If the result is an error, the behaviour is undefined.
     *
     * @return const ValueType&
     */
    inline const ValueType &get() const noexcept {
        assert(is_ok());
        return _stored;
    }

    /**
     * @brief Steal the stored value.
     * @note  If the result is an error, the behaviour is undefined.
     * @note  If T is reference type, the return value will be a reference wrapper.
     *
     * @return StoredType &&
     */
    inline StoredType &&steal_value() noexcept {
        assert(is_ok());
        return std::move(_stored);
    }

    /**
     * @brief Get a reference to the stored error.
     * @note  If the result is ok, the behaviour is undefined.
     *
     * @return Error&
     */
    inline Error &get_error() noexcept {
        assert(is_err());
        return *_error;
    }

    /**
     * @brief Get a reference to the stored error.
     * @note  If the result is ok, the behaviour is undefined.
     *
     * @return const Error&
     */
    inline const Error &get_error() const noexcept {
        assert(is_err());
        return *_error;
    }

    /**
     * @brief Steal the stored error.
     * @note  If the result is ok, the behaviour is undefined.
     *
     * @return ErrorType &&
     */
    inline ErrorType &&steal_error() noexcept {
        assert(is_err());
        return std::move(_error);
    }

protected:
    template <typename U>
    detail::Discriminant move_from(Result<U> &&other) {
        auto what = other._what;
        switch (what) {
        case detail::Discriminant::Ok:
            (void)new (&_stored) StoredType(std::move(other._stored));
            break;
        case detail::Discriminant::Err:
            (void)new (&_error) ErrorType(std::move(other._error));
            break;
        default:
            break;
        }
        return what;
    }

    void destroy() {
        switch (_what) {
        case detail::Discriminant::Ok:
            _stored.~StoredType();
            break;
        case detail::Discriminant::Err:
            _error.~ErrorType();
            break;
        default:
            break;
        }
    }

protected:
    union {
        StoredType _stored;
        ErrorType _error;
    };
    detail::Discriminant _what;
};

template <>
class [[nodiscard]] Result<void> : public ResultApi<Result<void>, void, Error> {
public:
    template <typename U>
    friend class Result;

    using StoredType = void;
    using ValueType = void;
    using DeclType = void;
    using ErrorType = std::unique_ptr<Error>;

    Result() : _error{nullptr} {}
    Result(Result &&other) { move_from(std::move(other)); }
    template <typename E, typename = std::enable_if_t<std::is_base_of_v<Error, E>>>
    Result(std::unique_ptr<E> &&e, NoStackFrameFlag) noexcept : _error{std::move(e)} {}
    template <typename E, typename = std::enable_if_t<std::is_base_of_v<Error, E>>>
    Result(std::unique_ptr<E> &&e, const SourceLocation &loc = SourceLocation::current()) noexcept
        : _error{std::move(e)} {
        _error->append_stack_frame(loc);
    }
    template <typename E>
    Result(ErrorResult<E> &&e, const SourceLocation &loc = SourceLocation::current()) noexcept
        : Result{e.steal_error(), loc} {}
    ~Result() { destroy(); }
    Result &operator=(Result &&other) {
        destroy();
        move_from(std::move(other));
        return *this;
    }
    template <typename U>
    Result &operator=(Result<U> &&other) {
        destroy();
        move_from(std::move(other));
        return *this;
    }

public:
    /* Predicates */

    /**
     * @brief Check whether the result is ok.
     *
     * @return true
     * @return false
     */
    [[nodiscard]] inline bool is_ok() const noexcept { return !_error; }

    /**
     * @brief Check whether the result is an error.
     *
     * @return true
     * @return false
     */
    [[nodiscard]] inline bool is_err() const noexcept { return !!_error; }

public:
    /* Accessors */

    /**
     * @brief Make interface compatible with Result<T>.
     */
    inline void get() const noexcept { assert(is_ok()); }

    /**
     * @brief Make interface compatible with Result<T>.
     */
    inline void steal_value() noexcept { assert(is_ok()); }

    /**
     * @brief Get a reference to the stored error.
     * @note  If the result is ok, the behaviour is undefined.
     *
     * @return Error&
     */
    inline Error &get_error() noexcept {
        assert(is_err());
        return *_error;
    }

    /**
     * @brief Get a reference to the stored error.
     * @note  If the result is ok, the behaviour is undefined.
     *
     * @return const Error&
     */
    inline const Error &get_error() const noexcept {
        assert(is_err());
        return *_error;
    }

    /**
     * @brief Steal the stored error.
     * @note  If the result is ok, the behaviour is undefined.
     *
     * @return ErrorType &&
     */
    inline ErrorType &&steal_error() noexcept {
        assert(is_err());
        return std::move(_error);
    }

protected:
    template <typename U>
    void move_from(Result<U> &&other) {
        if (other._error) {
            (void)new (&_error) ErrorType(std::move(other._error));
        } else {
            (void)new (&_error) ErrorType(nullptr);
        }
    }

    void destroy() {
        if (_error) {
            _error.~ErrorType();
        }
    }

protected:
    union {
        char _;
        ErrorType _error;
    };
};

#define AR_RET_IF_ERR(...) AR_RET_IF_ERR_IMPL_((__VA_ARGS__), )

#define AR_RET_IF_ERR_FWD(...)                                                                                         \
    AR_RET_IF_ERR_IMPL_1((__VA_ARGS__), , {std::move(__e), ::autil::result::NoStackFrameFlag()})

#define AR_RET_IF_ERR_IMPL_(e, hook_before_err_ret) AR_RET_IF_ERR_IMPL_1(e, hook_before_err_ret, __e)

#define AR_RET_IF_ERR_IMPL_1(e, hook_before_err_ret, ...)                                                              \
    ((typename ::autil::result::detail::ResultExprClassifier<decltype((e))>::ReturnType)({                             \
        auto __r = ::autil::result::detail::Resultify<decltype((e))>::Apply(e);                                        \
        if (unlikely(__r.is_err())) {                                                                                  \
            auto __e = __r.steal_error();                                                                              \
            do {                                                                                                       \
                hook_before_err_ret;                                                                                   \
            } while (0);                                                                                               \
            return __VA_ARGS__;                                                                                        \
        }                                                                                                              \
        __r.steal_value();                                                                                             \
    }))

#define AR_REQUIRE_TRUE(pred, err) AR_REQUIRE_TRUE_IMPL_(pred, err, )

#define AR_REQUIRE_TRUE_IMPL_(pred, err, hook_before_err_ret)                                                          \
    do {                                                                                                               \
        if (unlikely(!(bool)(pred))) {                                                                                 \
            do {                                                                                                       \
                hook_before_err_ret;                                                                                   \
            } while (0);                                                                                               \
            return err;                                                                                                \
        }                                                                                                              \
    } while (0)

} // namespace autil::result

namespace autil {
using result::Result;
using result::SourceLocation;
} // namespace autil
