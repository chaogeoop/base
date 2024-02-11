package io.github.chaogeoop.base.business.common.helpers;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CollectionHelper {
    public static boolean isEmpty(Collection<?> collections) {
        if (collections == null) {
            return true;
        }

        return collections.isEmpty();
    }

    public static <T> List<T> find(List<T> lists, Predicate<T> predicate) {
        if (lists == null) {
            return Lists.newArrayList();
        }
        return lists.stream().filter(predicate).collect(Collectors.toList());
    }

    public static <T> Set<T> find(Set<T> sets, Predicate<T> predicate) {
        if (sets == null) {
            return Sets.newHashSet();
        }

        return sets.stream().filter(predicate).collect(Collectors.toSet());
    }

    public static <T, R> List<R> map(List<T> lists, Function<T, R> mapper) {
        return lists.stream().map(mapper).collect(Collectors.toList());
    }

    public static <T, R> Set<R> map(Set<T> sets, Function<T, R> mapper) {
        return sets.stream().map(mapper).collect(Collectors.toSet());
    }

    public static <T, R> List<R> map(T[] array, Function<T, R> mapper) {
        return Lists.newArrayList(array).stream().map(mapper).collect(Collectors.toList());
    }

    public static <K, V> Map<K, V> toMap(List<V> lists, Function<V, K> keyGenerator) {
        Map<K, V> map = Maps.newHashMap();
        for (V value : lists) {
            K key = keyGenerator.apply(value);
            map.put(key, value);
        }
        return map;
    }

    public static <T> List<T> unique(List<T> lists) {
        return lists.stream().distinct().collect(Collectors.toList());
    }

    public static <T> List<T> remove(List<T> lists, Predicate<T> predicate) {
        List<T> removedList = Lists.newArrayList();
        lists.removeIf((t) -> {
            boolean result = predicate.test(t);
            if (result) {
                removedList.add(t);
            }
            return result;
        });
        return removedList;
    }

    public static <T> Set<T> remove(Set<T> sets, Predicate<T> predicate) {
        Set<T> removedSets = Sets.newHashSet();
        sets.removeIf((t) -> {
            boolean result = predicate.test(t);
            if (result) {
                removedSets.add(t);
            }
            return result;
        });
        return removedSets;
    }

    public static <T, K> Map<K, List<T>> groupBy(Collection<T> collections, Function<T, K> classifier) {
        return collections.stream().collect(Collectors.groupingBy(classifier));
    }

    public static <T> List<T> subList(List<T> data, int index, int len) {
        return data.stream().skip(index).limit(len).collect(Collectors.toList());
    }
}
