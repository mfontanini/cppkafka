/*
 * Copyright (c) 2016, Matias Fontanini
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following disclaimer
 *   in the documentation and/or other materials provided with the
 *   distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <algorithm>
#include <iostream>
#include <iomanip>
#include "buffer.h"

using std::string;
using std::equal;
using std::ostream;
using std::hex;
using std::dec;

namespace cppkafka {

Buffer::Buffer() 
: data_(nullptr), size_(0) {

}

Buffer::Buffer(const string& data) 
: Buffer(data.data(), data.size()) {

}

const Buffer::DataType* Buffer::get_data() const {
    return data_;
}

size_t Buffer::get_size() const {
    return size_;
}

Buffer::operator bool() const {
    return data_ != nullptr;
}

Buffer::operator string() const {
    return string(data_, data_ + size_);
}

bool Buffer::operator==(const Buffer& rhs) const {
    if (get_size() != rhs.get_size()) {
        return false;
    }
    return equal(get_data(), get_data() + get_size(), rhs.get_data());
}

bool Buffer::operator!=(const Buffer& rhs) const { 
    return !(*this == rhs);
}

ostream& operator<<(ostream& output, const Buffer& rhs) {
    for (size_t i = 0; i < rhs.get_size(); ++i) {
        char c = static_cast<char>(rhs.get_data()[i]);
        if (c >= ' ' && c < 127) {
            output << c;
        }
        else {
            output << "\\x";
            if (c < 16) {
                output << '0';
            }
            output << hex << (int)c << dec;
        }
    }
    return output;
}

} // cppkafka
