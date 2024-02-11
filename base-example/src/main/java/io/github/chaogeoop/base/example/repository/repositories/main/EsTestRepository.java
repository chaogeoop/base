package io.github.chaogeoop.base.example.repository.repositories.main;

import io.github.chaogeoop.base.example.repository.domains.EsTest;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.stereotype.Repository;

@Repository
public interface EsTestRepository extends QuerydslPredicateExecutor<EsTest> {
}
