package io.github.chaogeoop.base.business.mongodb;

import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class PersistProvider {
    private final MongoTemplate mongoTemplate;
    private final MongoTransactionManager mongoTransactionManager;
    private final MongoPersistEntity.AfterDbPersistInterface esSyncProvider;


    public PersistProvider(
            MongoTemplate mongoTemplate,
            MongoTransactionManager mongoTransactionManager,
            @Nullable MongoPersistEntity.AfterDbPersistInterface esSyncProvider
    ) {
        this.mongoTemplate = mongoTemplate;
        this.mongoTransactionManager = mongoTransactionManager;
        this.esSyncProvider = esSyncProvider;
    }

    public MongoTemplate giveMongoTemplate() {
        return this.mongoTemplate;
    }

    public void persist(
            List<MongoPersistEntity.PersistEntity> list
    ) {
        this.persist(list, persistMap -> {
        });
    }

    public void persist(
            List<MongoPersistEntity.PersistEntity> list,
            MongoPersistEntity.AfterDbPersistInterface afterDbPersistHandle
    ) {
        List<MongoPersistEntity.AfterDbPersistInterface> handlers = new ArrayList<>();
        if (this.esSyncProvider != null) {
            handlers.add(this.esSyncProvider);
        }
        handlers.add(afterDbPersistHandle);


        MongoPersistEntity entity = MongoPersistEntity.of(this.mongoTemplate, handlers);

        MongoPersistEntity.PersistMap persistMap = entity.convertToCollectionNameDatabaseMap(list);

        if (persistMap.databaseIsEmpty()) {
            entity.persist(persistMap);
        } else {
            TransactionStatus status = this.mongoTransactionManager.getTransaction(new DefaultTransactionDefinition());
            try {
                entity.persist(persistMap);
                this.mongoTransactionManager.commit(status);
            } catch (Exception e) {
                this.mongoTransactionManager.rollback(status);
                throw e;
            }
        }

        for (MongoPersistEntity.MessageInterface message : persistMap.getMessages()) {
            message.send();
        }
    }
}
