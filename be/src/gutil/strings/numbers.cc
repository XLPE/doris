// Copyright 2010 Google Inc. All Rights Reserved.
// Refactored from contributions of various authors in strings/strutil.cc
//
// This file contains string processing functions related to
// numeric values.

#include "gutil/strings/numbers.h"

#include <fmt/compile.h>
#include <fmt/format.h>

#include <cfloat>

#include "common/logging.h"
#include "gutil/integral_types.h"
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strtoint.h"

bool safe_strtof(const char* str, float* value) {
    char* endptr;
#ifdef _MSC_VER // has no strtof()
    *value = strtod(str, &endptr);
#else
    *value = strtof(str, &endptr);
#endif
    if (endptr != str) {
        while (ascii_isspace(*endptr)) ++endptr;
    }
    // Ignore range errors from strtod/strtof.
    // The values it returns on underflow and
    // overflow are the right fallback in a
    // robust setting.
    return *str != '\0' && *endptr == '\0';
}

bool safe_strtod(const char* str, double* value) {
    char* endptr;
    *value = strtod(str, &endptr);
    if (endptr != str) {
        while (ascii_isspace(*endptr)) ++endptr;
    }
    // Ignore range errors from strtod.  The values it
    // returns on underflow and overflow are the right
    // fallback in a robust setting.
    return *str != '\0' && *endptr == '\0';
}

bool safe_strtof(const string& str, float* value) {
    return safe_strtof(str.c_str(), value);
}

bool safe_strtod(const string& str, double* value) {
    return safe_strtod(str.c_str(), value);
}

// ----------------------------------------------------------------------
// SimpleDtoa()
// SimpleFtoa()
// DoubleToBuffer()
// FloatToBuffer()
//    We want to print the value without losing precision, but we also do
//    not want to print more digits than necessary.  This turns out to be
//    trickier than it sounds.  Numbers like 0.2 cannot be represented
//    exactly in binary.  If we print 0.2 with a very large precision,
//    e.g. "%.50g", we get "0.2000000000000000111022302462515654042363167".
//    On the other hand, if we set the precision too low, we lose
//    significant digits when printing numbers that actually need them.
//    It turns out there is no precision value that does the right thing
//    for all numbers.
//
//    Our strategy is to first try printing with a precision that is never
//    over-precise, then parse the result with strtod() to see if it
//    matches.  If not, we print again with a precision that will always
//    give a precise result, but may use more digits than necessary.
//
//    An arguably better strategy would be to use the algorithm described
//    in "How to Print Floating-Point Numbers Accurately" by Steele &
//    White, e.g. as implemented by David M. Gay's dtoa().  It turns out,
//    however, that the following implementation is about as fast as
//    DMG's code.  Furthermore, DMG's code locks mutexes, which means it
//    will not scale well on multi-core machines.  DMG's code is slightly
//    more accurate (in that it will never use more digits than
//    necessary), but this is probably irrelevant for most users.
//
//    Rob Pike and Ken Thompson also have an implementation of dtoa() in
//    third_party/fmt/fltfmt.cc.  Their implementation is similar to this
//    one in that it makes guesses and then uses strtod() to check them.
//    Their implementation is faster because they use their own code to
//    generate the digits in the first place rather than use snprintf(),
//    thus avoiding format string parsing overhead.  However, this makes
//    it considerably more complicated than the following implementation,
//    and it is embedded in a larger library.  If speed turns out to be
//    an issue, we could re-implement this in terms of their
//    implementation.
// ----------------------------------------------------------------------
int DoubleToBuffer(double value, int width, char* buffer) {
    // DBL_DIG is 15 for IEEE-754 doubles, which are used on almost all
    // platforms these days.  Just in case some system exists where DBL_DIG
    // is significantly larger -- and risks overflowing our buffer -- we have
    // this assert.
    COMPILE_ASSERT(DBL_DIG < 20, DBL_DIG_is_too_big);

    int snprintf_result = snprintf(buffer, width, "%.*g", DBL_DIG, value);

    // The snprintf should never overflow because the buffer is significantly
    // larger than the precision we asked for.
    DCHECK(snprintf_result > 0 && snprintf_result < width);

    if (strtod(buffer, nullptr) != value) {
        snprintf_result = snprintf(buffer, width, "%.*g", DBL_DIG + 2, value);

        // Should never overflow; see above.
        DCHECK(snprintf_result > 0 && snprintf_result < width);
    }

    return snprintf_result;
}

int FloatToBuffer(float value, int width, char* buffer) {
    // FLT_DIG is 6 for IEEE-754 floats, which are used on almost all
    // platforms these days.  Just in case some system exists where FLT_DIG
    // is significantly larger -- and risks overflowing our buffer -- we have
    // this assert.
    COMPILE_ASSERT(FLT_DIG < 10, FLT_DIG_is_too_big);

    int snprintf_result = snprintf(buffer, width, "%.*g", FLT_DIG, value);

    // The snprintf should never overflow because the buffer is significantly
    // larger than the precision we asked for.
    DCHECK(snprintf_result > 0 && snprintf_result < width);

    float parsed_value;
    if (!safe_strtof(buffer, &parsed_value) || parsed_value != value) {
        snprintf_result = snprintf(buffer, width, "%.*g", FLT_DIG + 2, value);

        // Should never overflow; see above.
        DCHECK(snprintf_result > 0 && snprintf_result < width);
    }

    return snprintf_result;
}

int FastDoubleToBuffer(double value, char* buffer) {
    auto end = fmt::format_to(buffer, FMT_COMPILE("{}"), value);
    *end = '\0';
    return end - buffer;
}

int FastFloatToBuffer(float value, char* buffer) {
    auto* end = fmt::format_to(buffer, FMT_COMPILE("{}"), value);
    *end = '\0';
    return end - buffer;
}

// ----------------------------------------------------------------------
// SimpleItoaWithCommas()
//    Description: converts an integer to a string.
//    Puts commas every 3 spaces.
//    Faster than printf("%d")?
//
//    Return value: string
// ----------------------------------------------------------------------

char* SimpleItoaWithCommas(int64_t i, char* buffer, int32_t buffer_size) {
    // 19 digits, 6 commas, and sign are good for 64-bit or smaller ints.
    char* p = buffer + buffer_size;
    // Need to use uint64 instead of int64 to correctly handle
    // -9,223,372,036,854,775,808.
    uint64 n = i;
    if (i < 0) n = 0 - n;
    *--p = '0' + n % 10; // this case deals with the number "0"
    n /= 10;
    while (n) {
        *--p = '0' + n % 10;
        n /= 10;
        if (n == 0) break;

        *--p = '0' + n % 10;
        n /= 10;
        if (n == 0) break;

        *--p = ',';
        *--p = '0' + n % 10;
        n /= 10;
        // For this unrolling, we check if n == 0 in the main while loop
    }
    if (i < 0) *--p = '-';
    return p;
}

char* SimpleItoaWithCommas(__int128_t i, char* buffer, int32_t buffer_size) {
    // 39 digits, 12 commas, and sign are good for 128-bit or smaller ints.
    char* p = buffer + buffer_size;
    // Need to use uint128 instead of int128 to correctly handle
    // -170,141,183,460,469,231,731,687,303,715,884,105,728.
    __uint128_t n = i;
    if (i < 0) n = 0 - n;
    *--p = '0' + n % 10; // this case deals with the number "0"
    n /= 10;
    while (n) {
        *--p = '0' + n % 10;
        n /= 10;
        if (n == 0) break;

        *--p = '0' + n % 10;
        n /= 10;
        if (n == 0) break;

        *--p = ',';
        *--p = '0' + n % 10;
        n /= 10;
        // For this unrolling, we check if n == 0 in the main while loop
    }
    if (i < 0) *--p = '-';
    return p;
}
