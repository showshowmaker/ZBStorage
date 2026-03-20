#pragma once

#include <algorithm>
#include <cstdint>
#include <string>

namespace zb::mds {

inline std::string NormalizeDecimalString(std::string value) {
    if (value.empty()) {
        return "0";
    }
    value.erase(0, std::min(value.find_first_not_of('0'), value.size() - 1U));
    if (value.empty()) {
        return "0";
    }
    return value;
}

inline int CompareDecimalStrings(const std::string& lhs, const std::string& rhs) {
    const std::string a = NormalizeDecimalString(lhs);
    const std::string b = NormalizeDecimalString(rhs);
    if (a.size() != b.size()) {
        return a.size() < b.size() ? -1 : 1;
    }
    if (a == b) {
        return 0;
    }
    return a < b ? -1 : 1;
}

inline std::string AddDecimalStrings(const std::string& lhs, const std::string& rhs) {
    std::string a = NormalizeDecimalString(lhs);
    std::string b = NormalizeDecimalString(rhs);
    if (a.size() < b.size()) {
        std::swap(a, b);
    }

    std::string out;
    out.reserve(a.size() + 1U);
    int carry = 0;
    size_t ai = a.size();
    size_t bi = b.size();
    while (ai > 0 || bi > 0 || carry != 0) {
        int digit = carry;
        if (ai > 0) {
            digit += a[--ai] - '0';
        }
        if (bi > 0) {
            digit += b[--bi] - '0';
        }
        out.push_back(static_cast<char>('0' + (digit % 10)));
        carry = digit / 10;
    }
    std::reverse(out.begin(), out.end());
    return NormalizeDecimalString(std::move(out));
}

inline std::string SubtractDecimalStrings(const std::string& lhs, const std::string& rhs) {
    if (CompareDecimalStrings(lhs, rhs) < 0) {
        return "0";
    }
    const std::string a = NormalizeDecimalString(lhs);
    const std::string b = NormalizeDecimalString(rhs);

    std::string out;
    out.reserve(a.size());
    int borrow = 0;
    size_t ai = a.size();
    size_t bi = b.size();
    while (ai > 0) {
        int digit = (a[--ai] - '0') - borrow;
        if (bi > 0) {
            digit -= (b[--bi] - '0');
        }
        if (digit < 0) {
            digit += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        out.push_back(static_cast<char>('0' + digit));
    }
    while (out.size() > 1U && out.back() == '0') {
        out.pop_back();
    }
    std::reverse(out.begin(), out.end());
    return NormalizeDecimalString(std::move(out));
}

inline uint64_t DivideDecimalStringByU64(const std::string& decimal, uint64_t divisor) {
    if (divisor == 0) {
        return 0;
    }
    const std::string normalized = NormalizeDecimalString(decimal);
    uint64_t result = 0;
    uint64_t remainder = 0;
    for (char ch : normalized) {
        const uint64_t digit = static_cast<uint64_t>(ch - '0');
        remainder = remainder * 10ULL + digit;
        if (result > (UINT64_MAX / 10ULL)) {
            return UINT64_MAX;
        }
        result *= 10ULL;
        const uint64_t quotient_digit = remainder / divisor;
        if (result > (UINT64_MAX - quotient_digit)) {
            return UINT64_MAX;
        }
        result += quotient_digit;
        remainder %= divisor;
    }
    return result;
}

class MasstreeDecimalAccumulator {
public:
    void Add(uint64_t delta) {
#if defined(__SIZEOF_INT128__)
        value_ += static_cast<unsigned __int128>(delta);
#else
        value_decimal_ = AddDecimalStrings(value_decimal_, std::to_string(delta));
#endif
    }

    std::string ToString() const {
#if defined(__SIZEOF_INT128__)
        unsigned __int128 value = value_;
        if (value == 0) {
            return "0";
        }
        std::string out;
        while (value != 0) {
            const unsigned int digit = static_cast<unsigned int>(value % 10);
            out.push_back(static_cast<char>('0' + digit));
            value /= 10;
        }
        std::reverse(out.begin(), out.end());
        return out;
#else
        return NormalizeDecimalString(value_decimal_);
#endif
    }

    uint64_t DivideBy(uint64_t divisor) const {
        return DivideDecimalStringByU64(ToString(), divisor);
    }

private:
#if defined(__SIZEOF_INT128__)
    unsigned __int128 value_{0};
#else
    std::string value_decimal_{"0"};
#endif
};

} // namespace zb::mds
