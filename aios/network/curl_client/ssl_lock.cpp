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
#include <cassert>
#include <curl_client/ssl_lock.h>
#include <iostream>
#include <mutex>
#include <openssl/conf.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <vector>
using namespace std;

namespace network {
AUTIL_LOG_SETUP(common, LibsslGlobalLock);

#define THREAD_ID pthread_self()
#define OPENSSL_1_1_API (OPENSSL_VERSION_NUMBER >= 0x1010000fL)
#define OPENSSL_1_1_1_API (OPENSSL_VERSION_NUMBER >= 0x10101000L)
#if OPENSSL_1_1_API
// CRYPTO_LOCK is deprecated as of OpenSSL 1.1.0
LibsslGlobalLock::LibsslGlobalLock() {
    AUTIL_LOG(INFO, "OpenSSL not OPENSSL_1_1_API not do CRYPTO_set_locking_callback %s", OPENSSL_VERSION_TEXT);
    init();
}
LibsslGlobalLock::~LibsslGlobalLock() {}
#else  // !OPENSSL_1_1_API
std::mutex *ssl_global_locks;
void ssl_locking_cb(int mode, int type, const char *file, int line) {
    if (mode & CRYPTO_LOCK) {
        ssl_global_locks[type].lock();
    } else {
        ssl_global_locks[type].unlock();
    }
}
static unsigned long id_function(void) { return ((unsigned long)THREAD_ID); }

LibsslGlobalLock::LibsslGlobalLock() {
    if (ssl_global_locks) {
        AUTIL_LOG(INFO, "OpenSSL global lock has been already set %s", OPENSSL_VERSION_TEXT);
        return;
    }
    AUTIL_LOG(INFO, "OpenSSL do CRYPTO_set_locking_callback %s", OPENSSL_VERSION_TEXT);
    init();
    ssl_global_locks = new std::mutex[CRYPTO_num_locks()];
    // CRYPTO_set_id_callback(ssl_thread_id); OpenSSL manual says that
    // if threadid_func is not specified using
    // CRYPTO_THREADID_set_callback(), then default implementation is
    // used. We use this default one.
    CRYPTO_set_locking_callback(ssl_locking_cb);
    CRYPTO_set_id_callback(id_function);
}
LibsslGlobalLock::~LibsslGlobalLock() {
    AUTIL_LOG(INFO, "OpenSSL do  LibsslGlobalLock destruct%s", OPENSSL_VERSION_TEXT);
    CRYPTO_set_locking_callback(nullptr);
}
#endif // !OPENSSL_1_1_API

void LibsslGlobalLock::init() {
#if OPENSSL_1_1_API
// No explicit initialization is required.
#elif defined(OPENSSL_IS_BORINGSSL)
    CRYPTO_library_init();
#else  // !OPENSSL_1_1_API && !defined(OPENSSL_IS_BORINGSSL)
    OPENSSL_config(nullptr);
    SSL_load_error_strings();
    SSL_library_init();
    OpenSSL_add_all_algorithms();
#endif // !OPENSSL_1_1_API && !defined(OPENSSL_IS_BORINGSSL)
}

} // namespace network
