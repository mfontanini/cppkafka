/*
 * Copyright (c) 2017, Matias Fontanini
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
 
#ifndef CPPKAFKA_TRAITS_H
#define CPPKAFKA_TRAITS_H

#include <type_traits>
#include <librdkafka/rdkafka.h>

#if (__cplusplus < 201402L)
namespace std {
    template< bool B, class T = void >
    using enable_if_t = typename enable_if<B,T>::type;
}
#endif

namespace cppkafka {

//librdkafka type mappings
template <typename T> struct rdkafka_type{};
template <> struct rdkafka_type<int> { using type = int; };
template <> struct rdkafka_type<bool> { using type = bool; };
template <> struct rdkafka_type<std::string> { using type = std::string; };

template <typename T>
struct is_rdkafka_type {
    template <typename U>
    static std::true_type test(typename rdkafka_type<U>::type*);

    template <typename U>
    static std::false_type test(...);

    static constexpr bool value = decltype(test<T>(nullptr))::value;
};

struct unknown;
template <int I> struct rdkafka_id_2_type{ using type = unknown; };
template <> struct rdkafka_id_2_type<RD_C_STR> { using type = std::string; };
template <> struct rdkafka_id_2_type<RD_C_S2I> { using type = std::string; };
template <> struct rdkafka_id_2_type<RD_C_S2F> { using type = std::string; };
template <> struct rdkafka_id_2_type<RD_C_PATLIST> { using type = std::string; };
template <> struct rdkafka_id_2_type<RD_C_KSTR> { using type = std::string; };
template <> struct rdkafka_id_2_type<RD_C_BOOL> { using type = bool; };
template <> struct rdkafka_id_2_type<RD_C_INT> { using type = int; };

template <int I, typename T>
struct rdkafka_type_match {
    static constexpr bool value = std::is_same<typename rdkafka_id_2_type<I>::type, T>::value;
};

struct GenericType{}; //legacy behavior
struct ConsumerType{};
struct ProducerType{};
template <typename T> class ConsumerHandle;
template <typename T> class ProducerHandle;
template <typename T> class HandleConfig;
template <typename T> class TopicConfig;
template <typename T> struct HandleTraits;

template <>
struct HandleTraits<GenericType>
{
    static constexpr int scope = RD_GLOBAL;
    static constexpr int topic_scope = RD_TOPIC;
    using traits_type = HandleTraits<GenericType>;
    using config_type = HandleConfig<traits_type>;
    using topic_config_type = TopicConfig<traits_type>;
};

template <>
struct HandleTraits<ConsumerType>
{
    static constexpr int scope = RD_CONSUMER;
    static constexpr int topic_scope = RD_TOPIC | scope;
    using traits_type = HandleTraits<ConsumerType>;
    using config_type = HandleConfig<traits_type>;
    using topic_config_type = TopicConfig<traits_type>;
};

template <>
struct HandleTraits<ProducerType>
{
    static constexpr int scope = RD_PRODUCER;
    static constexpr int topic_scope = RD_TOPIC | scope;
    using traits_type = HandleTraits<ProducerType>;
    using config_type = HandleConfig<traits_type>;
    using topic_config_type = TopicConfig<traits_type>;
};

//=============================================================================
//                             TRAIT HELPERS
//=============================================================================
/**
 * \brief Verifies if a type has a handle trait
 * \tparam T The type to check
 */
template <typename T>
struct has_handle_traits {
    template <typename U = typename std::decay<T>::type>
    static std::true_type test(typename U::traits_type*);

    template <typename U = typename std::decay<T>::type>
    static std::false_type test(...);

    static constexpr bool value = decltype(test<T>(nullptr))::value;
};

/**
 * \brief Internal template helper class
 * \tparam T Type to check
 * \tparam Trait The trait type it's not supposed to have
 */
template <typename T, typename Trait>
struct does_not_have_specific_handle_traits {
    template <typename U = typename std::decay<T>::type,
             typename = std::enable_if_t<has_handle_traits<U>::value &&
                                         !std::is_same<typename U::traits_type, Trait>::value>>
    static std::true_type test(typename U::traits_type*);

    template <typename U = typename std::decay<T>::type>
    static std::false_type test(...);

    static constexpr bool value = decltype(test<T>(nullptr))::value;
};

/**
 * \brief Verifies if a type has a producer trait
 * \tparam T The type to check
 * \returns TRUE if the type has a GenericTrait or a ProducerTrait
 */
template <typename T>
using has_producer_traits = does_not_have_specific_handle_traits<T, HandleTraits<ConsumerType>>;

/**
 * \brief Verifies if a type has a producer trait
 * \tparam T The type to check
 * \returns TRUE if the type has a GenericTrait or a ConsumerTrait
 */
template <typename T>
using has_consumer_traits = does_not_have_specific_handle_traits<T, HandleTraits<ProducerType>>;

} //namespace

#endif //CPPKAFKA_TRAITS_H
