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
#include <curl_client/callback_handler.h>
#include <curl_client/decoder.h>

namespace network {

size_t CallBackHandler::write_callback(void *data, size_t size, size_t nmemb, void *userdata) {
    DecoderPtr decoder = *(reinterpret_cast<DecoderPtr *>(userdata));
    return decoder->Decode(data, size, nmemb);
}

size_t CallBackHandler::header_callback(void *data, size_t size, size_t nmemb, void *userdata) {
    DecoderPtr decoder = *(reinterpret_cast<DecoderPtr *>(userdata));
    return decoder->DecodeHeader(data, size, nmemb);
}

size_t CallBackHandler::read_callback(void *data, size_t size, size_t nmemb, void *userdata) {
    UploadObject *rawData;
    rawData = reinterpret_cast<UploadObject *>(userdata);
    size_t curl_size = size * nmemb;
    size_t copy_size = (rawData->length < curl_size) ? rawData->length : curl_size;
    std::memcpy(data, rawData->data, copy_size);
    rawData->length -= copy_size;
    rawData->data += copy_size;
    return copy_size;
}

} // namespace network
