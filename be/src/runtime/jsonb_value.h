// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef DORIS_BE_RUNTIME_JSON_VALUE_H
#define DORIS_BE_RUNTIME_JSON_VALUE_H

#include <glog/logging.h>

#include <cstddef>
#include <ostream>
#include <string>

#include "common/status.h"
#include "util/hash_util.hpp"
#include "util/jsonb_parser_simd.h"

namespace doris {

struct JsonBinaryValue {
    static const int MAX_LENGTH = (1 << 30);

    // default nullprt and size 0 for invalid or NULL value
    const char* ptr = nullptr;
    size_t len = 0;
    JsonbParser parser;

    JsonBinaryValue() = default;

    const char* value() const { return ptr; }

    size_t size() const { return len; }

    void replace(const char* ptr, int len) {
        this->ptr = ptr;
        this->len = len;
    }

    bool operator==(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }
    // !=
    bool ne(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }
    // <=
    bool le(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }
    // >=
    bool ge(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }
    // <
    bool lt(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }
    // >
    bool gt(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }

    bool operator!=(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }

    bool operator<=(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }

    bool operator>=(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }

    bool operator<(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }

    bool operator>(const JsonBinaryValue& other) const {
        throw Exception(Status::FatalError("comparing between JsonBinaryValue is not supported"));
    }

    Status init_from_json_string(const char* s, size_t len);

    Status init_from_json_string(const std::string& s) {
        return init_from_json_string(s.c_str(), s.size());
    }

    std::string to_json_string() const;

    struct HashOfJsonBinaryValue {
        size_t operator()(const JsonBinaryValue& v) const {
            return HashUtil::hash(v.ptr, v.len, 0);
        }
    };
};

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const JsonBinaryValue& v) {
    return HashUtil::hash(v.ptr, v.len, 0);
}

std::ostream& operator<<(std::ostream& os, const JsonBinaryValue& json_value);

std::size_t operator-(const JsonBinaryValue& v1, const JsonBinaryValue& v2);

} // namespace doris

#endif
