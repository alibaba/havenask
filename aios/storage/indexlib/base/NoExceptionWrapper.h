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
#include <exception>
#include <functional>
#include <type_traits>

#include "indexlib/base/Status.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2 {

// transfer old indexlib calls from exception to Status
class NoExceptionWrapper
{
private:
    template <typename F, typename... Args>
    struct ReturnTraits {
        using ReturnType = typename std::invoke_result_t<F, Args...>;
    };
    template <typename F, typename... Args>
    static inline constexpr bool IsReturnVoid = std::is_void_v<typename ReturnTraits<F, Args...>::ReturnType>;

public:
    // auto [st, ret] = Invoke(foo, a, b, c);
    template <typename F, typename... Args>
    static std::pair<Status, std::enable_if_t<!IsReturnVoid<F, Args...>, typename ReturnTraits<F, Args...>::ReturnType>>
    Invoke(F&& f, Args&&... args)
    {
        using R = typename ReturnTraits<F, Args...>::ReturnType;
        try {
            return {Status::OK(), std::forward<F&&>(f)(std::forward<Args&&>(args)...)};
        } catch (const indexlib::util::FileIOException& e) {
            return {Status::IOError(e.what()), R()};
        } catch (const indexlib::util::SchemaException& e) {
            return {Status::ConfigError(e.what()), R()};
        } catch (const indexlib::util::InconsistentStateException& e) {
            return {Status::Corruption(e.what()), R()};
        } catch (const indexlib::util::RuntimeException& e) {
            return {Status::Corruption(e.what()), R()};
        } catch (const indexlib::util::BadParameterException& e) {
            return {Status::InvalidArgs(e.what()), R()};
        } catch (const indexlib::util::OutOfMemoryException& e) {
            return {Status::NoMem(e.what()), R()};
        } catch (const indexlib::util::ExistException& e) {
            return {Status::Exist(e.what()), R()};
        } catch (const indexlib::util::ExceptionBase& e) {
            return {Status::InternalError(e.what()), R()};
        } catch (const std::exception& e) {
            return {Status::Unknown("unknown exception", e.what()), R()};
        } catch (...) {
            return {Status::Unknown("unknown exception"), R()};
        }
    }

    // Status st = Invoke(fooReturnVoid, a, b, c);
    template <typename F, typename... Args>
    static std::enable_if_t<IsReturnVoid<F, Args...>, Status> Invoke(F&& f, Args&&... args)
    {
        auto [st, ret] = Invoke(
            [](F&& _f, Args&&... _args) -> std::pair<Status, bool> {
                std::forward<F&&>(_f)(std::forward<Args&&>(_args)...);
                return {Status::OK(), true};
            },
            std::forward<F&&>(f), std::forward<Args&&>(args)...);
        (void)ret;
        return std::move(st);
    }

    // auto [st, ret] = Invoke(&C::foo, a, b, c);
    // Status st = Invoke(&C::fooReturnVoid, a, b, c);
    template <typename M, typename C, typename... Args>
    static auto Invoke(M(C::*d), Args&&... args)
    {
        return Invoke([](M(C::*_d), Args&&... _args) { return std::mem_fn(_d)(std::forward<Args&&>(_args)...); }, d,
                      std::forward<Args&&>(args)...);
    }
};

} // namespace indexlibv2
